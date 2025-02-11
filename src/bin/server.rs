use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::{mem, ptr, thread};

use core_affinity::CoreId;
use shiftlock::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::*,
    sync::RwLock,
};

use clap::Parser;
use shiftlock::utils::RemoteInfo;
use rrddmma::{prelude::*, rdma::mr::Permission, wrap::RegisteredMem};

mod huge_alloc {
    use libc::*;
    use std::ptr;

    #[inline]
    fn alloc_mmap(len: usize, flags: i32) -> *mut u8 {
        // SAFETY: FFI.
        let ret = unsafe {
            mmap(
                ptr::null_mut(),
                len,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | flags,
                -1,
                0,
            ) as *mut u8
        };

        if ret != MAP_FAILED as _ {
            ret
        } else {
            ptr::null_mut()
        }
    }

    #[inline]
    fn alloc_memalign(len: usize, align: usize) -> *mut u8 {
        let mut ptr = ptr::null_mut();
        // SAFETY: FFI.
        let ret = unsafe { posix_memalign(&mut ptr, align, len) };
        if ret == 0 {
            ptr as _
        } else {
            ptr::null_mut()
        }
    }

    const HUGE_PAGE_SIZE: usize = 1 << 21;

    /// Allocate memory.
    pub(crate) fn alloc_raw(len: usize) -> *mut u8 {
        // Roundup to huge page size.
        let len = (len + HUGE_PAGE_SIZE - 1) & !(HUGE_PAGE_SIZE - 1);

        // 1. Try to allocate huge page.
        let ptr = alloc_mmap(len, MAP_HUGETLB);
        if !ptr.is_null() {
            return ptr;
        }

        eprintln!(
            "failed to mmap {}MB hugepages, trying normal pages; performance can be low.",
            len >> 20
        );

        // 2. Try to allocate normal page.
        let ptr = alloc_mmap(len, 0);
        if !ptr.is_null() {
            return ptr;
        }

        eprintln!(
            "failed to mmap {}MB normal pages, trying posix_memalign; performance can be low.",
            len >> 20
        );

        // 3. Try to posix_memalign, align to page size.
        let ptr = alloc_memalign(len, 1 << 12);
        if !ptr.is_null() {
            return ptr;
        }

        panic!("failed to allocate {}MB memory", len >> 20);
    }

    /// Allocate zeroed memory.
    pub(crate) fn alloc_zeroed(len: usize) -> *mut u8 {
        let buf = alloc_raw(len);
        if !buf.is_null() {
            // SAFETY: ptr is valid.
            unsafe { ptr::write_bytes(buf, 0, len) };
        }
        buf
    }
}

/// Lock server.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// UDP port to listen on.
    #[clap(short, long, default_value = "31850")]
    pub port: u16,

    /// RDMA device to use.
    #[clap(short, long, default_value = "mlx5_0")]
    pub dev: String,

    /// Number of RPC threads.
    #[clap(short, long, default_value = "47")]
    pub nthreads: usize,

    /// Lock entry size in bytes.
    /// Default to 16 for ShiftLock.
    #[clap(short, long, default_value = "16")]
    pub locksize: usize,

    /// Application logic entry size in bytes.
    /// Default to 64.
    #[clap(short, long, default_value = "64")]
    pub valsize: usize,

    /// Lock count.
    #[clap(short, long, default_value = "10000000")]
    pub count: usize,
}

struct Delegation {
    rcq: Cq,
    tx: mpsc::Sender<Qp>,
}

impl Delegation {
    fn new(rcq: Cq, tx: mpsc::Sender<Qp>) -> Self {
        Self { rcq, tx }
    }

    fn delegate(&self, qp: Qp) {
        self.tx.send(qp).unwrap();
    }
}

struct Info {
    delegations: Arc<Vec<Delegation>>,
    port: Port,
    pd: Pd,
    mr: MrRemote,
}

#[derive(Debug, Clone, Copy)]
struct OneSidedInfo {
    lock_size: usize,
    era_addr: *mut u64,
}

unsafe impl Send for OneSidedInfo {}
unsafe impl Sync for OneSidedInfo {}

struct RdmaInfo {
    /// Loopback QP.
    lbqp: Qp,
    lbmr: MrRemote,
    rcq: Cq,
}

use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::main(flavor = "current_thread")]
async fn rpc_handler(
    rdma: RdmaInfo,
    rx: mpsc::Receiver<Qp>,
    locks: Arc<Vec<RwLock<()>>>,
    one_sided: OneSidedInfo,
) {
    let RdmaInfo { lbqp, lbmr, rcq } = rdma;

    const RPC_MIN_LEN: usize = 2 * mem::size_of::<u64>();
    const RPC_LEN: usize = 4 * mem::size_of::<u64>();
    const RX_BATCH: usize = 16;

    let mut qps = Vec::new();
    let mut bufs = Vec::new();
    let mut guards = HashMap::new();

    let mem = RegisteredMem::new(lbqp.pd(), 1024).unwrap();

    #[allow(unused)]
    enum Guard<'a> {
        Write(tokio::sync::RwLockWriteGuard<'a, ()>),
        Read(tokio::sync::RwLockReadGuard<'a, ()>),
    }

    let mut wcs = vec![Wc::default(); RX_BATCH];
    loop {
        while let Ok(qp) = rx.try_recv() {
            let qp_idx = qps.len() as u64;
            let mut mem = RegisteredMem::new(qp.pd(), RPC_LEN * (RX_BATCH + 1)).unwrap();
            mem.as_mut().fill(0);

            for i in 0..RX_BATCH {
                qp.recv(
                    &[mem.slice((i + 1) * RPC_LEN, RPC_LEN).unwrap()],
                    (qp_idx << 32) | i as u64,
                )
                .expect("cannot post receive");
            }

            qps.push(qp);
            bufs.push(mem);
        }

        let n = rcq.poll_into(&mut wcs).expect("cannot poll receive CQ") as usize;
        if n == 0 {
            std::hint::spin_loop();
            continue;
        }

        for wc in wcs.iter().take(n) {
            if wc.status() != WcStatus::Success {
                eprintln!("unexpected WC status: {:?}", wc.status());
            }

            let len = wc.byte_len();
            assert!(len >= RPC_MIN_LEN);

            let qp_idx = (wc.wr_id() >> 32) as usize;
            let buf_idx = wc.wr_id() as u32 as usize;

            let buf = bufs[qp_idx]
                .slice((buf_idx + 1) * RPC_LEN, RPC_LEN)
                .unwrap();
            let ty = unsafe { ptr::read(buf.addr() as *const u64) };
            let lock_id = unsafe { ptr::read(buf.addr().add(mem::size_of::<u64>()) as *const u64) };

            let mut ok = true;
            match ty {
                LOCK_ACQUIRE_EX => {
                    let guard = locks[lock_id as usize].try_write();
                    if let Ok(guard) = guard {
                        guards.insert(lock_id, Guard::Write(guard));
                    } else {
                        ok = false;
                    }
                }
                LOCK_ACQUIRE_SH => {
                    let guard = locks[lock_id as usize].try_read();
                    if let Ok(guard) = guard {
                        guards.insert(lock_id, Guard::Read(guard));
                    } else {
                        ok = false;
                    }
                }
                LOCK_RELEASE => {
                    guards.remove(&lock_id);
                }
                LOCK_RECOVER => {
                    assert!(len >= RPC_MIN_LEN + mem::size_of::<u64>());
                    let req_era = unsafe {
                        ptr::read(buf.addr().add(2 * mem::size_of::<u64>()) as *const u64)
                    };

                    let era = unsafe { &*(one_sided.era_addr as *const AtomicU64) };
                    ok = era
                        .compare_exchange(req_era, req_era + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                    if ok {
                        let lock_addr = lock_id as *mut u64;
                        match one_sided.lock_size {
                            16 => {
                                let lock_entry = unsafe { &*(lock_addr as *const ShiftLockEntry) };
                                let remote = lbmr.slice_by_ptr(lock_addr as _, 16).unwrap();

                                let cur_rel_cnt = lock_entry.rel_cnt();
                                let new_rel_cnt = cur_rel_cnt ^ RELCNT_LEAP;
                                let compare_mask = [0, !0];
                                let compare = [0, cur_rel_cnt];
                                let swap_mask = [!0, !0];
                                let swap = [0, new_rel_cnt];

                                use rrddmma::rdma::qp::ExtCompareSwapParams;
                                fn ptr_to(val: &[u64; 2]) -> std::ptr::NonNull<u64> {
                                    (&val[0]).into()
                                }

                                let local_rd_buf = mem.as_slice();
                                let cas_params = ExtCompareSwapParams {
                                    compare: ptr_to(&compare),
                                    swap: ptr_to(&swap),
                                    compare_mask: ptr_to(&compare_mask),
                                    swap_mask: ptr_to(&swap_mask),
                                };
                                // SAFETY: pointers are all from references and are therefore valid.
                                unsafe {
                                    lbqp.ext_compare_swap::<16>(
                                        local_rd_buf,
                                        remote,
                                        cas_params,
                                        0,
                                        true,
                                    )
                                }
                                .expect("failed to perform Reset ExtCmpSwp");
                                lbqp.scq().poll_one_blocking_consumed();
                            }
                            _ => {
                                assert!(
                                    one_sided.lock_size <= 8,
                                    "invalid lock size {}",
                                    one_sided.lock_size
                                );
                                let lock_addr = unsafe { &*(lock_addr as *const AtomicU64) };
                                lock_addr.store(0, Ordering::SeqCst);
                            }
                        }
                    }
                }
                _ => {
                    eprintln!("unexpected RPC type: {}", ty);
                }
            }

            let qp = &qps[qp_idx];
            qp.recv(&[buf], wc.wr_id())
                .expect("cannot replenish receive");

            let send_buf = bufs[qp_idx].slice(0, 8).unwrap();
            unsafe {
                ptr::write(send_buf.addr() as *mut u64, if ok { 1 } else { 0 });
            }
            qp.send(&[send_buf], None, None, 0, true, true).unwrap();
            qp.scq().poll_one_blocking_consumed();
        }
    }
}

async fn handle_cli(id: usize, mut stream: TcpStream, addr: SocketAddr, info: Info) {
    let mut lenbuf = [0; 4];
    let mut buf = [0; 4096];

    // Read length.
    stream
        .read_exact(&mut lenbuf)
        .await
        .expect("cannot read length from client");
    let len = u32::from_le_bytes(lenbuf) as usize;

    // Read data.
    stream
        .read_exact(&mut buf[..len])
        .await
        .expect("cannot read data from client");

    let Ok(ep) = serde_json::from_slice::<QpEndpoint>(&buf[..len]) else {
        eprintln!(
            "invalid QpEndpoint received from {}: {:?}",
            addr,
            &buf[..len]
        );
        return;
    };

    let scq = Cq::new(info.pd.context(), Cq::DEFAULT_CQ_DEPTH).expect("cannot create CQ");
    let rcq = &info.delegations[id % info.delegations.len()].rcq;
    let mut qp = Qp::builder()
        .qp_type(QpType::Rc)
        .caps(QpCaps::default())
        .send_cq(&scq)
        .recv_cq(rcq)
        .sq_sig_all(false)
        .global_routing(false)
        .enable_feature(rrddmma::rdma::qp::ExpFeature::ExtendedAtomics)
        .build(&info.pd)
        .expect("cannot create QP");
    qp.bind_local_port(&info.port, None)
        .expect("cannot bind local port");
    qp.bind_peer(ep).expect("cannot bind peer");

    let remote_info = RemoteInfo {
        ep: qp.endpoint().unwrap(),
        mr: info.mr,
    };
    let remote_info = serde_json::to_vec(&remote_info).expect("cannot serialize my RemoteInfo");

    // Send back the QP info only when the QP is ready.
    // Write length.
    let len = remote_info.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .await
        .expect("cannot write length to client");
    stream
        .write_all(&remote_info)
        .await
        .expect("cannot write data to client");

    info.delegations[id % info.delegations.len()].delegate(qp);
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let len = args.locksize * (args.count + EXTRA_COUNT as usize);
    let len = len + (4096 - len % 4096);
    let len = len + args.valsize * args.count;
    println!(
        "Memory buffer: [{} * {}] .. [{} * {}] = {:.2} MB",
        args.locksize,
        args.count,
        args.valsize,
        args.count,
        len as f64 / 1e6
    );

    let buf = huge_alloc::alloc_zeroed(len);
    let era_addr = unsafe { buf.add(args.locksize * args.count) };
    assert!(era_addr as usize % 8 == 0);

    let one_sided = OneSidedInfo {
        lock_size: args.locksize,
        era_addr: era_addr as *mut u64,
    };

    let Nic { context, ports } = Nic::finder()
        .dev_name(args.dev)
        .probe()
        .expect("cannot find or open device");
    let pd = Pd::new(&context).expect("cannot create PD");
    let reg_mem =
        unsafe { Mr::reg(&pd, buf, len, Permission::default()) }.expect("cannot register memory");

    let locks = Arc::new((0..args.count).map(|_| RwLock::new(())).collect::<Vec<_>>());
    let mut rxs = Vec::new();
    let delegations = Arc::new(
        (1..=args.nthreads)
            .map(|_| {
                let cq = Cq::new(&context, Cq::DEFAULT_CQ_DEPTH).unwrap();
                let (tx, rx) = mpsc::channel();
                rxs.push(rx);
                Delegation::new(cq, tx)
            })
            .collect::<Vec<_>>(),
    );

    // Spawn handler threads.
    let _handlers = rxs
        .into_iter()
        .enumerate()
        .map(|(i, rx)| {
            let locks = locks.clone();
            let cq = delegations[i].rcq.clone();

            // Build a loopback QP for one-sided recovery.
            let qp = {
                let cq = Cq::new(&context, Cq::DEFAULT_CQ_DEPTH).unwrap();
                let mut qp = Qp::builder()
                    .qp_type(QpType::Rc)
                    .caps(QpCaps::default())
                    .send_cq(&cq)
                    .recv_cq(&cq)
                    .sq_sig_all(false)
                    .global_routing(false)
                    .enable_feature(rrddmma::rdma::qp::ExpFeature::ExtendedAtomics)
                    .build(&pd)
                    .expect("cannot create QP");
                qp.bind_local_port(&ports[0], None)
                    .expect("cannot bind local port");
                qp.bind_peer(qp.endpoint().unwrap())
                    .expect("cannot bind peer");
                qp
            };
            let lbmr = MrRemote::from(reg_mem.as_slice());
            let rdma_info = RdmaInfo {
                lbqp: qp,
                lbmr,
                rcq: cq,
            };

            thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: i + 1 });
                rpc_handler(rdma_info, rx, locks, one_sided);
            });
        })
        .collect::<Vec<_>>();

    // Create a TCP socket and listen on it.

    let addr = format!("0.0.0.0:{}", args.port);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("cannot bind TCP socket");

    println!("Listening on: {} ...", addr);
    let mut cnt = 0;

    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .expect("cannot accept TCP connection");

        let rdma_info = Info {
            delegations: delegations.clone(),
            port: ports[0].clone(),
            pd: pd.clone(),
            mr: MrRemote::from(reg_mem.as_slice()),
        };
        tokio::spawn(handle_cli(cnt, stream, addr, rdma_info));
        cnt += 1;
    }
}
