use std::{
    fs, io, mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tokio::sync::Barrier;

use clap::Parser;
use core_affinity::CoreId;
use shiftlock::{app::*, baselines::*, utils::*, *};
use quanta::Instant;
use rand::prelude::*;
use rrddmma::{prelude::*, wrap::RegisteredMem};

use crate::utils::make_connected_qp;
use crate::EXTRA_COUNT;

/// Lock type.
#[derive(strum::EnumString, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    ShiftLock,
    Cas,
    Mcs,
    Dslr,
    Drtm,
    Rmarw,
    Rpc,
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

    /// Lock type.
    #[clap(short, long, default_value = "ShiftLock")]
    pub lock: LockType,

    /// Lock count.
    #[clap(short, long, default_value = "10000000")]
    pub count: u64,

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
    stats: Arc<Vec<Mutex<Timer>>>,
}

impl<P> RunArgs<P> {
    fn new(
        id: usize,
        nworkers: usize,
        policy: P,
        count: u64,
        workload: Workload,
        stats: Arc<Vec<Mutex<Timer>>>,
    ) -> Self {
        Self {
            id,
            nworkers,
            policy,
            count,
            workload,
            stats,
        }
    }
}

const LOCAL_MEM_LEN: usize = 1 << 20;
const BATCH: u64 = 64;
const MAX_LOCKS_PER_TXN: usize = 16;
const MAX_TX_TYPES: usize = 7;

#[tokio::main(flavor = "current_thread")]
async fn run_shiftlock(
    netargs: NetArgs,
    runargs: RunArgs<ShiftLockAcquirePolicy>,
    barrier: Arc<Barrier>,
) -> u64 {
    const LOCK_RESERVE: usize = 256;
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");

    let mut dci = utils::make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = (0..MAX_LOCKS_PER_TXN)
        .map(|i| {
            let dct = utils::make_dct_together_with(&qp, true);
            ShiftLock::initiate_dct(&dct, mem.slice(i * LOCK_RESERVE, LOCK_RESERVE).unwrap())
                .expect("cannot post initial DCT recvs");
            dct
        })
        .collect::<Vec<_>>();
    let node_id = qp.port().unwrap().0.lid();

    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<ShiftLockEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    if barrier.wait().await.is_leader() {
        let era_loc = remote.addr + mem::size_of::<ShiftLockEntry>() as u64 * runargs.count;
        ShiftLock::initiate_era(era_loc);
        println!("all threads ready, running...");
    }

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
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
                            lock_idx as usize * mem::size_of::<ShiftLockEntry>(),
                            mem::size_of::<ShiftLockEntry>(),
                        )
                        .unwrap();
                    let lock = ShiftLock::new(node_id, mem.as_slice(), remote_lock);

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        if shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, &mut dci, &dct[0], runargs.policy)
                                .await
                        }
                        .unwrap()
                    };
                    guard.release(&qp, &mut dci, &dct[0]);
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * LOCK_RESERVE, LOCK_RESERVE).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<ShiftLockEntry>(),
                            mem::size_of::<ShiftLockEntry>(),
                        )
                        .unwrap();

                    let lock = ShiftLock::new(node_id, local, remote);
                    guards.push(
                        if instr.shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, &mut dci, &dct[j], runargs.policy)
                                .await
                        }
                        .unwrap(),
                    );
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
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for (j, guard) in guards.into_iter().enumerate() {
                    guard.release(&qp, &mut dci, &dct[j]);
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_dslr(
    netargs: NetArgs,
    runargs: RunArgs<DslrAcquirePolicy>,
    barrier: Arc<Barrier>,
) -> u64 {
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<DslrEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    let local = mem.slice(0, 16).unwrap();

    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
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
                            lock_idx as usize * mem::size_of::<DslrEntry>(),
                            mem::size_of::<DslrEntry>(),
                        )
                        .unwrap();
                    let lock = DslrLock::new(local, remote_lock);

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        if shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, runargs.policy).await
                        }
                        .unwrap()
                    };
                    guard.release(&qp);
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * 16, 16).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<DslrEntry>(),
                            mem::size_of::<DslrEntry>(),
                        )
                        .unwrap();

                    let lock = DslrLock::new(local, remote);
                    guards.push(
                        if instr.shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, runargs.policy).await
                        }
                        .unwrap(),
                    );
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
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for guard in guards.into_iter() {
                    guard.release(&qp);
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_mcs(netargs: NetArgs, runargs: RunArgs<()>, barrier: Arc<Barrier>) -> u64 {
    const LOCK_RESERVE: usize = 256;
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");

    let dci = utils::make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = (0..MAX_LOCKS_PER_TXN)
        .map(|i| {
            let dct = utils::make_dct_together_with(&qp, false);
            let local = mem.slice(i * LOCK_RESERVE, LOCK_RESERVE).unwrap();

            // Keep two recvs into the DCT SRQ (for notification from prev and next).
            for j in 0..2 {
                let local = local.slice((j + 1) * 8, 8).unwrap();
                dct.srq().recv(&[local], j as _).unwrap();
            }
            dct
        })
        .collect::<Vec<_>>();
    let node_id = qp.port().unwrap().0.lid();

    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<McsEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    const LOCK_SIZE: usize = mem::size_of::<McsEntry>() + mem::size_of::<u64>();

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
    let start = Instant::now();
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, .. } => {
            while start.elapsed() < runargs.workload.duration {
                let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let remote_lock = remote
                        .slice(lock_idx as usize * LOCK_SIZE, LOCK_SIZE)
                        .unwrap();
                    let lock = McsLock::new(node_id, mem.as_slice(), remote_lock);

                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        lock.acquire(&qp, &dci, &dct[0]).await
                    };
                    guard.release(&qp, &dci, &dct[0]).await;
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * LOCK_RESERVE, LOCK_RESERVE).unwrap();
                    let remote_lock = remote
                        .slice(instr.id as usize * LOCK_SIZE, LOCK_SIZE)
                        .unwrap();

                    let lock = McsLock::new(node_id, local, remote_lock);
                    guards.push(lock.acquire(&qp, &dci, &dct[j]).await);
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
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for (j, guard) in guards.into_iter().enumerate() {
                    guard.release(&qp, &dci, &dct[j]).await;
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_cas(
    netargs: NetArgs,
    mut runargs: RunArgs<CasAcquirePolicy>,
    barrier: Arc<Barrier>,
) -> u64 {
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<u64>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    const BACKOFF_UPDATE_PERIOD: Duration = Duration::from_millis(1);

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
    let start = Instant::now();
    let mut backoff_last_update = start;
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, .. } => {
            let local = mem.slice(0, 8).unwrap();
            let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();

            while start.elapsed() < runargs.workload.duration {
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let remote_lock = remote
                        .slice(
                            lock_idx as usize * mem::size_of::<u64>(),
                            mem::size_of::<u64>(),
                        )
                        .unwrap();
                    let lock = CasLock::new(local, remote_lock);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        lock.acquire(&qp, &mut runargs.policy).await
                    };
                    guard.release(&qp).await;
                }
                thpt += BATCH;

                if backoff_last_update.elapsed() >= BACKOFF_UPDATE_PERIOD {
                    let CasWaitPolicy::TruncExpBackoff(ref mut backoff) = runargs.policy.wait
                    else {
                        continue;
                    };
                    backoff.update();
                    backoff_last_update = Instant::now();
                }
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, lock) in txn.locks().iter().enumerate() {
                    let remote_lock = remote
                        .slice(
                            lock.id as usize * mem::size_of::<DslrEntry>(),
                            mem::size_of::<DslrEntry>(),
                        )
                        .unwrap();
                    let lock = CasLock::new(mem.slice(j * 256, 256).unwrap(), remote_lock);
                    guards.push(lock.acquire(&qp, &mut runargs.policy).await);
                }

                // Application.
                {
                    let app_local = mem.slice(LOCAL_MEM_LEN / 2, 64).unwrap();
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        MicroRo::new(app_local, app_remote).run(&qp);
                    }
                    if !think_time.is_zero() {
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for guard in guards {
                    guard.release(&qp).await;
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;

                if backoff_last_update.elapsed() >= BACKOFF_UPDATE_PERIOD {
                    let CasWaitPolicy::TruncExpBackoff(backoff) = &mut runargs.policy.wait else {
                        continue;
                    };
                    backoff.update();
                    backoff_last_update = Instant::now();
                }
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
#[allow(unused)]
async fn run_drtm(netargs: NetArgs, runargs: RunArgs<u64>, barrier: Arc<Barrier>) -> u64 {
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let cli_id = qp.port().unwrap().0.lid() as u8;

    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<DrtmEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    let local = mem.slice(0, 16).unwrap();

    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
    let start = Instant::now();
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, read_ratio } => {
            let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();
            while start.elapsed() < runargs.workload.duration {
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let local_lock = mem.slice(0, 8).unwrap();
                    let remote_lock = remote
                        .slice(
                            lock_idx as usize * mem::size_of::<DrtmEntry>(),
                            mem::size_of::<DrtmEntry>(),
                        )
                        .unwrap();
                    let lock = DrtmLock::new(cli_id, local_lock, remote_lock);

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        if shared {
                            lock.acquire_sh(&qp, runargs.policy).await
                        } else {
                            lock.acquire_ex(&qp, runargs.policy).await
                        }
                    };
                    guard.release(&qp);
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * 8, 8).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<DrtmEntry>(),
                            mem::size_of::<DrtmEntry>(),
                        )
                        .unwrap();

                    let lock = DrtmLock::new(cli_id, local, remote);
                    guards.push(if false {
                        lock.acquire_sh(&qp, runargs.policy).await
                    } else {
                        lock.acquire_ex(&qp, runargs.policy).await
                    });
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
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                let mut ok = true;
                for (j, guard) in guards.into_iter().enumerate() {
                    ok = ok && guard.valid();
                    guard.release(&qp);
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                if ok {
                    thpt += 1;
                }
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_rmarw(netargs: NetArgs, runargs: RunArgs<()>, barrier: Arc<Barrier>) -> u64 {
    const LOCK_RESERVE: usize = 256;
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");

    let dci = utils::make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = (0..MAX_LOCKS_PER_TXN)
        .map(|i| {
            let dct = utils::make_dct_together_with(&qp, false);
            RmaRwLock::initiate_dct(&dct, mem.slice(i * LOCK_RESERVE, LOCK_RESERVE).unwrap())
                .expect("cannot post initial DCT recvs");
            dct
        })
        .collect::<Vec<_>>();
    let node_id = qp.port().unwrap().0.lid();

    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<RmaRwLockEntry>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
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
                            lock_idx as usize * mem::size_of::<RmaRwLockEntry>(),
                            mem::size_of::<RmaRwLockEntry>(),
                        )
                        .unwrap();
                    let lock = RmaRwLock::new(node_id, mem.as_slice(), remote_lock);

                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        if shared {
                            lock.acquire_sh(&qp).await
                        } else {
                            lock.acquire_ex(&qp, &dci, &dct[0]).await
                        }
                    };
                    guard.release(&qp, &dci, &dct[0]);
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let local = mem.slice(j * LOCK_RESERVE, LOCK_RESERVE).unwrap();
                    let remote = remote
                        .slice(
                            instr.id as usize * mem::size_of::<RmaRwLockEntry>(),
                            mem::size_of::<RmaRwLockEntry>(),
                        )
                        .unwrap();

                    let lock = RmaRwLock::new(node_id, local, remote);
                    guards.push(if instr.shared {
                        lock.acquire_sh(&qp).await
                    } else {
                        lock.acquire_ex(&qp, &dci, &dct[j]).await
                    });
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
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for (j, guard) in guards.into_iter().enumerate() {
                    guard.release(&qp, &dci, &dct[j]);
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
}

#[tokio::main(flavor = "current_thread")]
async fn run_rpc(netargs: NetArgs, runargs: RunArgs<()>, barrier: Arc<Barrier>) -> u64 {
    const LOCK_RESERVE: usize = 256;
    let (qp, remote) = make_connected_qp(&netargs.dev, netargs.server);
    let app_offset = (runargs.count + EXTRA_COUNT) as usize * mem::size_of::<u64>();
    let app_offset = app_offset + (4096 - app_offset % 4096);

    let mem = RegisteredMem::new(qp.pd(), LOCAL_MEM_LEN).expect("cannot register memory");
    if barrier.wait().await.is_leader() {
        println!("all threads ready, running...");
    }

    let mut timer = (0..MAX_TX_TYPES).map(|_| Timer::new()).collect::<Vec<_>>();
    let mut thpt = 0;
    let start = Instant::now();
    match runargs.workload.ty {
        WorkloadType::MicroZipf { theta, read_ratio } => {
            let local = mem.slice(0, 256).unwrap();
            let zipf = rand_distr::Zipf::new(runargs.count, theta as f64).unwrap();

            while start.elapsed() < runargs.workload.duration {
                for _ in 0..BATCH {
                    let lock_idx =
                        (runargs.count - 1).min(rand::thread_rng().sample(zipf) as u64 - 1);
                    let lock = RpcLock::new(local, lock_idx);
                    let shared = rand::thread_rng().gen_bool(read_ratio);
                    let guard = {
                        let _op = TimerOp::new(&mut timer[0]);
                        lock.acquire(&qp, shared).await
                    };
                    guard.release(&qp);
                }
                thpt += BATCH;
            }
        }
        WorkloadType::Trace { trace, think_time } => {
            let mut i = runargs.id;
            while start.elapsed() < runargs.workload.duration {
                let txn = &trace.txns()[i];
                let mut guards = Vec::with_capacity(txn.locks().len());

                let op = TimerOp::new(&mut timer[txn.ty as usize]);
                for (j, instr) in txn.locks().iter().enumerate() {
                    let lock = RpcLock::new(
                        mem.slice(j * LOCK_RESERVE, LOCK_RESERVE).unwrap(),
                        instr.id as _,
                    );
                    guards.push(lock.acquire(&qp, instr.shared).await);
                }

                // Application.
                {
                    let app_local = mem.slice(LOCAL_MEM_LEN / 2, 64).unwrap();
                    for lock in txn.locks() {
                        let app_remote = remote
                            .slice(app_offset + lock.id as usize * 64, 64)
                            .unwrap();
                        MicroRo::new(app_local, app_remote).run(&qp);
                    }
                    if !think_time.is_zero() {
                        async_busy_wait_dur(think_time).await;
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
                drop(op);

                for guard in guards {
                    guard.release(&qp);
                }

                i = (i + runargs.nworkers) % trace.txns().len();
                thpt += 1;
            }
        }
    }
    for (i, timer) in timer.into_iter().enumerate() {
        runargs.stats[i].lock().unwrap().merge(timer);
    }
    thpt
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
    let mut args = Args::parse();
    if args.lock == LockType::Mcs {
        eprintln!("using MCS lock, ignoring backoff");
    }
    if args.count == 0 {
        eprintln!("count must be positive, using 1 lock");
        args.count = 1;
    }

    let workload = parse_workload(&args.workload);
    eprintln!("{:?}", workload);

    let n = args.nthreads;
    let barrier = Arc::new(Barrier::new(n));
    let mut threads = Vec::new();
    let stats = Arc::new(
        (0..MAX_TX_TYPES)
            .map(|_| Mutex::new(Timer::new()))
            .collect::<Vec<_>>(),
    );

    let start = Instant::now();
    for i in 0..n {
        let barrier = barrier.clone();

        let netargs = NetArgs::new(args.dev.clone(), args.server);
        let count = args.count;
        let backoff = args.backoff;
        let workload = workload.clone();
        let stats = stats.clone();

        threads.push(thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: i });
            match args.lock {
                LockType::ShiftLock => {
                    let policy = ShiftLockAcquirePolicy {
                        remote_wait: match backoff {
                            0 => None,
                            _ => Some(backoff),
                        },
                    };
                    let thpt = run_shiftlock(
                        netargs,
                        RunArgs::new(i, n, policy, count, workload, stats),
                        barrier,
                    );
                    timing::commit();
                    consrec::commit();
                    thpt
                }
                LockType::Dslr => {
                    let policy = match backoff {
                        0 => DslrAcquirePolicy::BusyPoll,
                        _ => DslrAcquirePolicy::DynamicWait { nanos: backoff },
                    };
                    let thpt = run_dslr(
                        netargs,
                        RunArgs::new(i, n, policy, count, workload, stats),
                        barrier,
                    );
                    timing::commit();
                    thpt
                }
                LockType::Mcs => run_mcs(
                    netargs,
                    RunArgs::new(i, n, (), count, workload, stats),
                    barrier,
                ),
                LockType::Cas => {
                    let policy = CasAcquirePolicy {
                        wait: match backoff {
                            0 => CasWaitPolicy::BusyPoll,
                            _ => CasWaitPolicy::TruncExpBackoff(Backoff::new(backoff)),
                        },
                        retry: CasRetryPolicy::Cas,
                    };
                    let thpt = run_cas(
                        netargs,
                        RunArgs::new(i, n, policy, count, workload, stats),
                        barrier,
                    );
                    timing::commit();
                    thpt
                }
                LockType::Drtm => run_drtm(
                    netargs,
                    RunArgs::new(i, n, backoff, count, workload, stats),
                    barrier,
                ),
                LockType::Rmarw => run_rmarw(
                    netargs,
                    RunArgs::new(i, n, (), count, workload, stats),
                    barrier,
                ),
                LockType::Rpc => run_rpc(
                    netargs,
                    RunArgs::new(i, n, (), count, workload, stats),
                    barrier,
                ),
            }
        }));
    }

    let tot = threads.into_iter().map(|t| t.join().unwrap()).sum::<u64>();
    let secs = start.elapsed().as_secs_f64();

    println!("throughput: {:.1} ops/s", tot as f64 / secs);

    for timer in stats.iter() {
        let mut timer = timer.lock().unwrap();
        if timer.is_empty() {
            break;
        }
        let report = timer.report();
        if let Some(ref output) = args.output {
            report
                .append_to_file(output, secs)
                .expect("cannot write to output file");
        } else {
            println!("{:?}", report);
        }
    }

    let out: &mut dyn io::Write = if let Some(ref output) = args.output {
        &mut fs::File::options()
            .append(true)
            .create(true)
            .open(output)
            .unwrap()
    } else {
        &mut io::stdout()
    };

    if matches!(
        args.lock,
        LockType::Cas | LockType::Dslr | LockType::ShiftLock
    ) {
        timing::report(out);
    }

    if args.lock == LockType::ShiftLock {
        let conswrts = consrec::get();
        for (i, v) in conswrts.iter().enumerate().skip(1) {
            writeln!(out, "conswrt {}: {}", i, *v).unwrap();
        }

        let n = conswrts.iter().sum::<usize>();
        let avg = conswrts
            .iter()
            .enumerate()
            .map(|(i, v)| i * v)
            .sum::<usize>() as f64
            / n as f64;
        writeln!(out, "conswrt avg: {:.2}", avg).unwrap();
        writeln!(out, "handover: {}", consrec::get_handover_cnt()).unwrap();
    }
}
