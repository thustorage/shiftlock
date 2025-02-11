//! A hello-world example for ShiftLock.

use core_affinity::CoreId;
use rand::Rng;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use clap::Parser;
use quanta::Instant;
use rrddmma::{ctrl::Connecter, prelude::*, wrap::RegisteredMem};
use shiftlock::*;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Number of threads.
    #[clap(short, long, default_value = "8")]
    pub nthreads: usize,

    /// Run duration in seconds.
    #[clap(short, long, default_value = "10")]
    pub duration: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn worker(
    #[allow(unused)] id: usize,
    barrier: Arc<Barrier>,
    qp: Qp,
    remote: MrRemote,
    dur: Duration,
) -> u64 {
    let mut dci = make_qp_together_with(&qp, QpType::DcIni, Some(qp.scq().clone()));
    let dct = make_dct_together_with(&dci, true);

    const LEN: usize = 1 << 8;
    let mem = RegisteredMem::new(qp.pd(), LEN).expect("cannot register memory");
    ShiftLock::initiate_dct(&dct, mem.as_slice()).expect("cannot post initial DCT recvs");

    let id = dci.port().unwrap().0.lid();
    let mut lock = ShiftLock::new(id, mem.as_slice(), remote);
    let mut n = 0;
    barrier.wait();

    let policy = ShiftLockAcquirePolicy {
        remote_wait: Some(1000),
    };

    const BATCH: u64 = 64;
    let start = Instant::now();
    while start.elapsed() < dur {
        for _ in 0..BATCH {
            let guard = if rand::thread_rng().gen_bool(0.8) {
                lock.acquire_sh(&qp, policy).await
            } else {
                lock.acquire_ex(&qp, &mut dci, &dct, policy).await
            }
            .unwrap();
            lock = guard.release(&qp, &mut dci, &dct);
        }
        n += BATCH;
    }
    n
}

fn main() {
    eprintln!("NOTE: this is a hello-world example for ShiftLock.");
    eprintln!("      for real distributed benchmarking, run `server` and `client`.");

    let args = Args::parse();
    let qp = make_qp("mlx5_0", QpType::Rc);

    const LEN: usize = 1 << 10;
    let mem = RegisteredMem::new(qp.pd(), LEN).expect("cannot register memory");
    let remote = MrRemote::from(mem.slice_by_range(0..16).unwrap());

    let barrier = Arc::new(Barrier::new(args.nthreads));
    let mut handles = Vec::with_capacity(args.nthreads);
    let mut svr_qps = Vec::with_capacity(args.nthreads);
    for i in 0..args.nthreads {
        let core_id = 12 + (i / 12) * 24 + (i % 12);
        let barrier = barrier.clone();

        let mut svr_qp = make_qp_together_with(&qp, QpType::Rc, None);
        let mut cli_qp = make_qp("mlx5_0", QpType::Rc);
        Connecter::connect_local(&mut cli_qp, &mut svr_qp).unwrap();
        svr_qps.push(svr_qp);

        let handle = thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: core_id });
            worker(
                i,
                barrier,
                cli_qp,
                remote,
                Duration::from_secs(args.duration),
            )
        });
        handles.push(handle);
    }

    eprintln!("Running for {} seconds...", args.duration);

    let mut total = 0;
    for handle in handles {
        let x = handle.join().unwrap();
        // println!("{x}");
        total += x;
    }
    println!(
        "OK: {} threads: {} locks/s",
        args.nthreads,
        total as f64 / args.duration as f64
    );
}
