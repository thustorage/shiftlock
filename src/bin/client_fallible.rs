use std::{
    mem,
    net::SocketAddr,
    sync::{mpsc::Sender, Arc},
    thread,
    time::Duration,
};
use tokio::sync::Barrier;

use clap::Parser;
use core_affinity::CoreId;
use handlock::{app::*, baselines::*, utils::*, *};
use quanta::Instant;
use rand::prelude::*;
use rrddmma::{prelude::*, wrap::RegisteredMem};

use crate::utils::make_connected_qp;

/// Lock type.
#[derive(strum::EnumString, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Handlock,
    Dslr,
    TryRecover,
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Server address.
    #[clap(short, long, default_value = "127.0.0.1:31850")]
    pub server: SocketAddr,

    /// Number of threads.
    #[clap(short, long, default_value = "1")]
    pub nthreads: usize,

    /// Workload configuration YAML file path.
    #[clap(short, long, default_value = "")]
    pub workload: String,

    /// Probability of failure.
    #[clap(short, long, default_value = "0.0")]
    pub failprob: f64,

    /// Lock type.
    #[clap(short, long, default_value = "Handlock")]
    pub lock: LockType,

    /// Backoff nanoseconds. 0 indicates busy retry.
    #[clap(short, long, default_value = "1862")]
    pub backoff: u64,

    /// RDMA device to use.
    #[clap(short, long, default_value = "mlx5_0")]
    pub dev: String,

    /// Result output file.
    #[clap(short, long, default_value = None)]
    pub output: Option<String>,
}

const LOCK_COUNT: u64 = 10_000_000;

struct NetArgs {
    dev: String,
    server: SocketAddr,
}

impl NetArgs {
    fn new(dev: String, server: SocketAddr) -> Self {
        Self { dev, server }
    }
}

#[derive(Debug, Clone)]
enum WorkloadType {
    /// Microbenchmark
    MicroZipf { theta: f32, read_ratio: f64 },

    /// Trace
    Trace {
        trace: Arc<Trace>,
        think_time: Duration,
    },
}

#[derive(Debug, Clone)]
struct Workload {
    ty: WorkloadType,
    duration: Duration,
}

struct RunArgs<P> {
    id: usize,
    nworkers: usize,

    policy: P,
    count: u64,
    workload: Workload,
    failprob: f64,
}

impl<P> RunArgs<P> {
    fn new(
        id: usize,
        nworkers: usize,
        policy: P,
        count: u64,
        workload: Workload,
        failprob: f64,
    ) -> Self {
        Self {
            id,
            nworkers,
            policy,
            count,
            workload,
            failprob,
        }
    }
}

const LOCAL_MEM_LEN: usize = 1 << 20;
const BATCH: u64 = 64;
const MAX_LOCKS_PER_TXN: usize = 16;

#[tokio::main(flavor = "current_thread")]
async fn run_handlock(
    netargs: NetArgs,
    runargs: RunArgs<HandlockAcquirePolicy>,
    barrier: Arc<Barrier>,
    tx: Sender<u64>,
) {
    const LOCK_RESERVE: usize = 256;
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");

    let mut dci = utils::make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = (0..MAX_LOCKS_PER_TXN)
        .map(|i| {
            let dct = utils::make_dct_together_with(&qp, true);
            Handlock::initiate_dct(&dct, mem.slice(i * LOCK_RESERVE, LOCK_RESERVE).unwrap())
                .expect("cannot post initial DCT recvs");
            dct
        })
        .collect::<Vec<_>>();
    let node_id = qp.port().unwrap().0.lid();

    let app_offset = runargs.count as usize * mem::size_of::<HandlockEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    if barrier.wait().await.is_leader() {
        let era_loc = remote.addr + mem::size_of::<HandlockEntry>() as u64 * runargs.count;
        Handlock::initiate_era(era_loc);
        println!("all threads ready, running...");
    }

    let mut thpt = 0;
    let mut sent = false;
    let start = Instant::now();
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, read_ratio } => {
            let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();
            while start.elapsed() < runargs.workload.duration {
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let remote_lock = remote
                        .slice(
                            lock_idx as usize * mem::size_of::<HandlockEntry>(),
                            mem::size_of::<HandlockEntry>(),
                        )
                        .unwrap();

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = loop {
                        let lock = Handlock::new(node_id, mem.as_slice(), remote_lock);
                        let g = if shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, &mut dci, &dct[0], runargs.policy)
                                .await
                        };
                        if let Some(guard) = g {
                            break guard;
                        }
                    };

                    if !rand::thread_rng().gen_bool(runargs.failprob) {
                        guard.release(&qp, &mut dci, &dct[0]);
                    }
                }
                thpt += BATCH;

                if start.elapsed() >= runargs.workload.duration && !sent {
                    tx.send(thpt).unwrap();
                    sent = true;
                } else if start.elapsed() >= runargs.workload.duration * 2 {
                    break;
                }
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            loop {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * LOCK_RESERVE, LOCK_RESERVE).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<HandlockEntry>(),
                            mem::size_of::<HandlockEntry>(),
                        )
                        .unwrap();

                    let guard = loop {
                        let lock = Handlock::new(node_id, local, remote);
                        let g = if instr.shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, &mut dci, &dct[j], runargs.policy)
                                .await
                        };
                        if let Some(guard) = g {
                            break guard;
                        }
                    };
                    guards.push(guard);
                }

                // Application.
                {
                    let app_local = mem.slice(txn.locks().len() * LOCK_RESERVE, 64).unwrap();
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        MicroRo::new(app_local, app_remote).run(&qp);
                    }
                    if !think_time.is_zero() {
                        busy_wait_dur(think_time);
                    }
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        if !lock.shared {
                            MicroWo::new(app_local, app_remote).run(&qp);
                        }
                    }
                }

                if !rand::thread_rng().gen_bool(runargs.failprob) {
                    for (j, guard) in guards.into_iter().enumerate() {
                        guard.release(&qp, &mut dci, &dct[j]);
                    }
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;

                if start.elapsed() >= runargs.workload.duration && !sent {
                    tx.send(thpt).unwrap();
                    sent = true;
                } else if start.elapsed() >= runargs.workload.duration * 2 {
                    break;
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
#[allow(unused)]
async fn run_dslr(
    netargs: NetArgs,
    runargs: RunArgs<DslrAcquirePolicy>,
    barrier: Arc<Barrier>,
) -> u64 {
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let app_offset = runargs.count as usize * mem::size_of::<DslrEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    let local = mem.slice(0, 16).unwrap();

    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    const RETRIES: usize = 256;

    let mut thpt = 0;
    let start = Instant::now();
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, read_ratio } => {
            let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();
            while start.elapsed() < runargs.workload.duration {
                let mut delta = BATCH;
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let remote_lock = remote
                        .slice(
                            lock_idx as usize * mem::size_of::<DslrEntry>(),
                            mem::size_of::<DslrEntry>(),
                        )
                        .unwrap();

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let mut cnt = 0;
                    let guard = loop {
                        let lock = DslrLock::new(local, remote_lock);
                        let g = if shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, runargs.policy).await
                        };
                        if g.is_some() {
                            break g;
                        } else {
                            cnt += 1;
                            if cnt == RETRIES {
                                break None;
                            }
                        }
                    };
                    let Some(guard) = guard else {
                        delta -= 1;
                        continue;
                    };
                    if !rand::thread_rng().gen_bool(runargs.failprob) {
                        guard.release(&qp);
                    }
                }
                thpt += delta;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let mut ok = true;
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * 16, 16).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<HandlockEntry>(),
                            mem::size_of::<HandlockEntry>(),
                        )
                        .unwrap();

                    let mut cnt = 0;
                    let guard = loop {
                        let lock = DslrLock::new(local, remote);
                        let g = if instr.shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, runargs.policy).await
                        };
                        if g.is_some() {
                            break g;
                        } else {
                            cnt += 1;
                            if cnt == RETRIES {
                                break None;
                            }
                        }
                    };
                    let Some(guard) = guard else {
                        ok = false;
                        break;
                    };
                    guards.push(guard);
                }
                if !ok {
                    for (j, guard) in guards.into_iter().enumerate() {
                        guard.release(&qp);
                    }
                    continue;
                }

                // Application.
                {
                    let app_local = mem.slice(txn.locks().len() * 8, 64).unwrap();
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        MicroRo::new(app_local, app_remote).run(&qp);
                    }
                    if !think_time.is_zero() {
                        busy_wait_dur(think_time);
                    }
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        if !lock.shared {
                            MicroWo::new(app_local, app_remote).run(&qp);
                        }
                    }
                }

                if !rand::thread_rng().gen_bool(runargs.failprob) {
                    for (j, guard) in guards.into_iter().enumerate() {
                        guard.release(&qp);
                    }
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_tryrecover(netargs: NetArgs) {
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    let remote_lock = remote.slice(0, mem::size_of::<HandlockEntry>()).unwrap();
    let node_id = qp.port().unwrap().0.lid();

    let lock = Handlock::new(node_id, mem.as_slice(), remote_lock);
    lock.request_reset(&qp);
}

fn parse_workload(workload: &str) -> Workload {
    if workload.is_empty() {
        eprintln!("no workload specified, using default (micro:zipf,wi:5)");
        return Workload {
            ty: WorkloadType::MicroZipf {
                theta: 0.99,
                read_ratio: 0.50,
            },
            duration: Duration::from_secs(5),
        };
    }

    let tokens = workload.split(':').collect::<Vec<_>>();
    if tokens.len() != 3 {
        panic!("invalid workload format, expected `<TYPE>:<ARG>:<DURATION>`");
    }
    let secs = tokens[2].parse::<u64>().expect("invalid duration");
    let ty = match tokens[0] {
        "micro" => {
            let micro_args = tokens[1].split(',').collect::<Vec<_>>();
            if micro_args.len() != 2 {
                panic!("invalid micro workload format, expected `<RWTYPE>,<DISTR>`");
            }
            let theta = match micro_args[0] {
                "uniform" => 0.0,
                "zipf" => 0.99,
                _ => {
                    let theta = micro_args[1].parse::<f32>().expect("invalid theta");
                    if !(0.0..=1.0).contains(&theta) {
                        panic!("theta must be in [0, 1]");
                    }
                    theta
                }
            };
            let read_ratio = match micro_args[1] {
                "wo" => 0.00,
                "wi" => 0.50,
                "ri" => 0.95,
                "ro" => 1.00,
                _ => {
                    let read_ratio = micro_args[0].parse::<f64>().expect("invalid read ratio");
                    if !(0.0..=1.0).contains(&read_ratio) {
                        panic!("read ratio must be in [0, 1]");
                    }
                    read_ratio
                }
            };

            WorkloadType::MicroZipf { theta, read_ratio }
        }
        "trace" => {
            let trace = Arc::new(Trace::load(tokens[1]).unwrap());
            let think_time = if tokens[1].contains("tatp") {
                Duration::from_nanos(2800)
            } else if tokens[1].contains("tpcc") {
                Duration::from_nanos(7000)
            } else {
                Duration::ZERO
            };
            WorkloadType::Trace { trace, think_time }
        }
        _ => {
            panic!("invalid workload type: {}", tokens[0]);
        }
    };
    Workload {
        ty,
        duration: Duration::from_secs(secs),
    }
}

fn main() {
    let args = Args::parse();

    let workload = parse_workload(&args.workload);
    eprintln!("{:?}", workload);

    let n = args.nthreads;
    let barrier = Arc::new(Barrier::new(n));
    let mut threads = Vec::new();

    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..n {
        let barrier = barrier.clone();

        let netargs = NetArgs::new(args.dev.clone(), args.server);
        let backoff = args.backoff;
        let workload = workload.clone();
        let failprob = args.failprob;

        let tx = tx.clone();

        threads.push(thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: i });
            match args.lock {
                LockType::Handlock => {
                    let policy = HandlockAcquirePolicy {
                        remote_wait: Some(backoff),
                    };
                    run_handlock(
                        netargs,
                        RunArgs::new(i, n, policy, LOCK_COUNT, workload, failprob),
                        barrier,
                        tx,
                    );
                    return 0;
                }
                LockType::Dslr => {
                    let policy = match backoff {
                        0 => DslrAcquirePolicy::BusyPoll,
                        _ => DslrAcquirePolicy::DynamicWait { nanos: backoff },
                    };
                    run_dslr(
                        netargs,
                        RunArgs::new(i, n, policy, LOCK_COUNT, workload, failprob),
                        barrier,
                    )
                }
                LockType::TryRecover => {
                    run_tryrecover(netargs);
                    return 0;
                }
            }
        }));
    }

    if args.lock == LockType::Handlock {
        drop(tx);
        let mut thpt = 0;
        for _ in 0..n {
            thpt += rx.recv().unwrap();
        }
        println!();
        println!(
            "throughput: {:.1} ops/s",
            thpt as f64 / workload.duration.as_secs_f64()
        );
        return;
    }

    let tot = threads.into_iter().map(|t| t.join().unwrap()).sum::<u64>();
    let secs = workload.duration.as_secs_f64();

    println!();
    println!("throughput: {:.1} ops/s", tot as f64 / secs);
}
