//! Implementation of HPDC'16 RMA-RW.

use std::ptr::NonNull;
use std::{mem, ptr};

use bitvec::{field::BitField, prelude as bv};
use rrddmma::prelude::*;
use rrddmma::rdma::qp::ExtCompareSwapParams;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(align(16))]
pub struct RmaRwLockEntry(bv::BitArr!(for 128));

impl_lock_basic_methods!(RmaRwLockEntry, 128);

impl RmaRwLockEntry {
    define_field_accessor!(tail_node, u16, 0..16, WITH_MASK);
    define_field_accessor!(tail_dct, u32, 16..40, WITH_MASK);
    define_field_accessor!(arrive, u32, 64..96, WITH_MASK);
    define_field_accessor!(depart, u32, 96..128, WITH_MASK);
}

impl std::fmt::Debug for RmaRwLockEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RmaRwLockEntry")
            .field("tail_node", &self.tail_node())
            .field("tail_dct", &self.tail_dct())
            .field("arrive", &self.arrive())
            .field("depart", &self.depart())
            .finish()
    }
}

const WRITE_FLAG: u32 = 1 << 31;

const ENTIRE_BYTES: usize = mem::size_of::<RmaRwLockEntry>();
const HALF_BYTES: usize = ENTIRE_BYTES / 2;
const NOTI_BUF_OFFSET: usize = ENTIRE_BYTES + mem::size_of::<Notification>() * 2;

const WRITE_THRESHOLD: u32 = crate::shiftlock::MODE_CHANGE_THRESHOLD;
const READER_THRESHOLD: u32 = 4000;

/// Remote RMA-RW lock instance.
#[derive(Debug)]
pub struct RmaRwLock<'a> {
    id: u16,
    buf: MrSlice<'a>,
    remote: MrRemote,
}

/// An acquired RMA-RW lock instance.
#[derive(Debug)]
pub struct RmaRwLockGuard<'a> {
    /// The original lock.
    lock: RmaRwLock<'a>,

    /// Lock type.
    shared: bool,

    /// Successor.
    succ: Option<(u16, u32)>,

    /// Number of consecutive writers.
    cons_wrt_cnt: u32,
}

/// Packed notification data among clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Notification {
    Successor { node: u16, dct: u32 },
    Handover { cons_wrt_cnt: u32 },
    ModeChanged,
}

impl RmaRwLock<'_> {
    /// Poll the DCT SRQ for notifications.
    /// Return an optional notification from predecessor and an optional notification from successor.
    #[inline(always)]
    fn poll_dct(&self, dct: &Dct) -> (Option<Notification>, Option<(u16, u32)>) {
        let mut pred = None;
        let mut succ = None;

        let mut wc = [Wc::default(); 2];
        let n = dct.cq().poll_into(&mut wc).expect("failed to poll DCT CQ");

        for wc in wc[..n as usize].iter() {
            debug_assert_eq!(wc.status(), WcStatus::Success);
            let id = wc.wr_id();
            let offset = ENTIRE_BYTES + id as usize * mem::size_of::<Notification>();

            // SAFETY: buffer pointer validity verified during creation.
            let noti = unsafe { ptr::read(self.buf.addr().add(offset) as *const Notification) };

            // Replenish the receive buffer.
            let recv_buf = self
                .buf
                .slice(offset, mem::size_of::<Notification>())
                .unwrap();
            dct.srq()
                .recv(&[recv_buf], id)
                .expect("failed to post DCT SRQ recv");

            // Each type of notification may only appear once.
            match noti {
                Notification::Successor { node, dct } => {
                    debug_assert!(succ.is_none());
                    succ = Some((node, dct));
                }
                _ => {
                    debug_assert!(pred.is_none());
                    pred = Some(noti);
                }
            };
        }
        (pred, succ)
    }

    async fn check_marker(&self, qp: &Qp) {
        let local_dc_buf = self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
        let remote_dc_buf = self
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();
        loop {
            qp.read(&[local_dc_buf], &remote_dc_buf, 0, true)
                .expect("failed to read counters in check_marker");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            let entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };
            let diff = entry.arrive().wrapping_sub(entry.depart());
            // eprintln!(" {} - {} = {diff}", entry.arrive(), entry.depart());
            // tokio::time::sleep(Duration::from_secs(1)).await;
            if diff == 0 || diff == WRITE_FLAG {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    fn change_marker(&self, qp: &Qp) {
        let local_dc_buf = self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
        let remote_dc_buf = self
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();
        qp.fetch_add(local_dc_buf, remote_dc_buf, WRITE_FLAG as u64, 0, true)
            .expect("failed to set write flag in change_marker");
        qp.scq().poll_one_blocking_consumed();
    }

    async fn check_and_wait_for_write(&self, qp: &Qp) {
        self.check_marker(qp).await;
        self.change_marker(qp);
        self.check_marker(qp).await;
    }

    fn reset_marker(&self, qp: &Qp) {
        let local_dc_buf = self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
        let remote_dc_buf = self
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();
        qp.read(&[local_dc_buf], &remote_dc_buf, 0, true)
            .expect("failed to read counters in check_marker");
        qp.scq().poll_one_blocking_consumed();

        // SAFETY: buffer pointer validity verified during creation.
        let entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };

        let mut sub_arrive = entry.depart();
        let sub_depart = entry.depart();
        if entry.arrive() >= WRITE_FLAG {
            sub_arrive += WRITE_FLAG;
        }

        let add = ((0u32.wrapping_sub(sub_arrive)) as u64)
            | ((0u32.wrapping_sub(sub_depart) as u64) << 32);
        let mask = 1 << 31;
        unsafe {
            qp.ext_fetch_add::<HALF_BYTES>(
                local_dc_buf,
                remote_dc_buf,
                NonNull::from(&add),
                NonNull::from(&mask),
                0,
                true,
            )
        }
        .expect("failed to reset marker");
        qp.scq().poll_one_blocking_consumed();
    }
}

impl<'a> RmaRwLock<'a> {
    pub fn new(id: u16, buf: MrSlice<'a>, remote: MrRemote) -> Self {
        // Remote lock pointer validity.
        debug_assert!(remote.addr % mem::align_of::<RmaRwLockEntry>() as u64 == 0);
        debug_assert_eq!(remote.len, ENTIRE_BYTES);

        // Local buffer validity.
        debug_assert!(buf.addr() as usize % 8 == 0);
        debug_assert!(buf.len() >= ENTIRE_BYTES + 3 * mem::size_of::<Notification>());

        Self { id, buf, remote }
    }

    /// Initiate the DCT SRQ with `recv`s.
    /// The user must call this function once initially before any lock acquisition.
    pub fn initiate_dct(dct: &Dct, buf: MrSlice) -> std::io::Result<()> {
        assert!(buf.addr() as usize % 8 == 0);
        assert!(buf.len() >= NOTI_BUF_OFFSET + mem::size_of::<Notification>());

        for i in 0..2 {
            let recv_buf = buf
                .slice(
                    ENTIRE_BYTES + i * mem::size_of::<Notification>(),
                    mem::size_of::<Notification>(),
                )
                .unwrap();
            dct.srq().recv(&[recv_buf], i as _)?;
        }
        Ok(())
    }

    pub async fn acquire_ex(self, qp: &Qp, dci: &Qp, dct: &Dct) -> RmaRwLockGuard<'a> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };

        // Issue an ExtCmpSwp to the lock entry, adding myself to the end of the MCS queue.
        let compare_mask = 0;
        let swap_mask = !0;
        let compare = 0;
        let swap = bit_repr_of!(RmaRwLockEntry: {
            tail_node: self.id,
            tail_dct: dct.dct_num(),
        });

        let cas_params = ExtCompareSwapParams {
            compare: NonNull::from(&compare),
            swap: NonNull::from(&swap),
            compare_mask: NonNull::from(&compare_mask),
            swap_mask: NonNull::from(&swap_mask),
        };
        let local_tail_buf = self.buf.slice_by_range(0..HALF_BYTES).unwrap();
        let remote_tail_buf = self.remote.slice_by_range(0..HALF_BYTES).unwrap();

        // SAFETY: pointers are all from references and are therefore valid.
        unsafe {
            qp.ext_compare_swap::<HALF_BYTES>(local_tail_buf, remote_tail_buf, cas_params, 0, true)
        }
        .expect("failed to perform initial ExtCmpSwp");
        qp.scq().poll_one_blocking_consumed();

        // SAFETY: buffer pointer validity verified during creation.
        let entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };

        let mut succ = None;
        let mut cons_wrt_cnt = 0;
        if entry.tail_node() == 0 && entry.tail_dct() == 0 {
            self.check_and_wait_for_write(qp).await;
        } else {
            // Sanity check: predecessor is not self.
            debug_assert!(entry.tail_node() != self.id || entry.tail_dct() != dct.dct_num());

            // Tell the previous node that I am acquiring the lock.
            let pred_ep = QpEndpoint::new(
                None,
                entry.tail_node(),
                qp.port().unwrap().0.num(),
                entry.tail_dct(),
            );
            let pred_peer = dci.make_peer(pred_ep).unwrap();

            // Notify predecessor.
            let noti_buf = self
                .buf
                .slice(NOTI_BUF_OFFSET, mem::size_of::<Notification>())
                .unwrap();
            let noti = Notification::Successor {
                node: self.id,
                dct: dct.dct_num(),
            };
            // SAFETY: buffer pointer validity verified during creation.
            unsafe { ptr::write(noti_buf.addr() as *mut Notification, noti) };

            dci.send(&[noti_buf], Some(&pred_peer), None, 0, true, true)
                .expect("failed to send to predecessor");
            dci.scq().poll_one_blocking_consumed();

            loop {
                let (pred, succ2) = self.poll_dct(dct);
                debug_assert!(
                    succ.and(succ2).is_none(),
                    "received duplicate Successor notifications"
                );
                succ = succ.or(succ2);

                if let Some(pred) = pred {
                    match pred {
                        Notification::Handover { cons_wrt_cnt: cwc } => cons_wrt_cnt = cwc,
                        Notification::ModeChanged => self.check_and_wait_for_write(qp).await,
                        _ => unreachable!(),
                    }
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        RmaRwLockGuard {
            lock: self,
            shared: false,
            succ,
            cons_wrt_cnt,
        }
    }

    pub async fn acquire_sh(self, qp: &Qp) -> RmaRwLockGuard<'a> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };

        let local_tail_buf = self.buf.slice_by_range(0..HALF_BYTES).unwrap();
        let remote_tail_buf = self.remote.slice_by_range(0..HALF_BYTES).unwrap();
        let local_dc_buf = self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
        let remote_dc_buf = self
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();

        let mut barrier = false;
        loop {
            if barrier {
                loop {
                    qp.read(&[local_dc_buf], &remote_dc_buf, 0, true)
                        .expect("failed to read counters");
                    qp.scq().poll_one_blocking_consumed();

                    // SAFETY: buffer pointer validity verified during creation.
                    let entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };
                    if entry.arrive() <= READER_THRESHOLD {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }

            let add = 1;
            let mask = 1 << 31;
            unsafe {
                qp.ext_fetch_add::<HALF_BYTES>(
                    local_dc_buf,
                    remote_dc_buf,
                    NonNull::from(&add),
                    NonNull::from(&mask),
                    0,
                    true,
                )
            }
            .expect("failed to increment ARRIVE counter");
            qp.scq().poll_one_blocking_consumed();

            let mut entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };
            if entry.arrive() >= READER_THRESHOLD {
                barrier = true;
                if entry.arrive() == READER_THRESHOLD {
                    qp.read(&[local_tail_buf], &remote_tail_buf, 0, true)
                        .expect("failed to read tail");
                    qp.scq().poll_one_blocking_consumed();
                    entry = unsafe { ptr::read(self.buf.addr() as *const RmaRwLockEntry) };

                    if entry.tail_node() == 0 && entry.tail_dct() == 0 {
                        self.reset_marker(qp);
                        barrier = false;
                    }
                }
                qp.fetch_add(local_dc_buf, remote_dc_buf, 0u64.wrapping_sub(1), 0, true)
                    .expect("failed to decrement ARRIVE counter");
                qp.scq().poll_one_blocking_consumed();

                tokio::task::yield_now().await;
                continue;
            }
            break;
        }

        RmaRwLockGuard {
            lock: self,
            shared: true,
            succ: None,
            cons_wrt_cnt: 0,
        }
    }
}

impl RmaRwLockGuard<'_> {
    fn release_ex(&self, qp: &Qp, dci: &Qp, dct: &Dct) {
        let cons_wrt_cnt = self.cons_wrt_cnt + 1;
        let mut is_reset = false;
        let mut is_modechange = false;

        if cons_wrt_cnt == WRITE_THRESHOLD {
            self.lock.reset_marker(qp);
            is_reset = true;
            is_modechange = true;
        }

        let mut succ = self.succ;
        if succ.is_none() {
            if !is_reset {
                self.lock.reset_marker(qp);
                is_modechange = true;
            }

            let local_tail_buf = self.lock.buf.slice_by_range(0..HALF_BYTES).unwrap();
            let remote_tail_buf = self.lock.remote.slice_by_range(0..HALF_BYTES).unwrap();
            let current = bit_repr_of!(RmaRwLockEntry: {
                tail_node: self.lock.id,
                tail_dct: dct.dct_num(),
            });
            qp.compare_swap(local_tail_buf, remote_tail_buf, current, 0, 0, true)
                .expect("failed to perform unlock CAS");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            let entry = unsafe { ptr::read(self.lock.buf.addr() as *const RmaRwLockEntry) };
            if entry.tail_node() == self.lock.id && entry.tail_dct() == dct.dct_num() {
                return;
            }

            succ = loop {
                let (pred, succ) = self.lock.poll_dct(dct);
                debug_assert!(pred.is_none(), "received unexpected notification");
                if succ.is_some() {
                    break succ;
                }
                std::hint::spin_loop();
            };
        }
        let (succ_node, succ_dctnum) = succ.unwrap();

        // Handover the lock or notify mode change to the successor.
        let pred_ep = QpEndpoint::new(None, succ_node, qp.port().unwrap().0.num(), succ_dctnum);
        let pred_peer = dci
            .make_peer(pred_ep)
            .unwrap_or_else(|_| panic!("failed to create peer for ep {:?}", pred_ep));

        let noti_buf = self
            .lock
            .buf
            .slice(NOTI_BUF_OFFSET, mem::size_of::<Notification>())
            .unwrap();
        let noti = if is_modechange {
            Notification::ModeChanged
        } else {
            Notification::Handover { cons_wrt_cnt }
        };
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { ptr::write(noti_buf.addr() as *mut Notification, noti) };

        dci.send(&[noti_buf], Some(&pred_peer), None, 0, true, true)
            .expect("failed to send to predecessor");
        dci.scq().poll_one_blocking_consumed();
    }

    fn release_sh(&self, qp: &Qp) {
        let local_dc_buf = self
            .lock
            .buf
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();
        let remote_dc_buf = self
            .lock
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();

        let add = 1 << 32;
        let mask = 1 << 31;
        unsafe {
            qp.ext_fetch_add::<HALF_BYTES>(
                local_dc_buf,
                remote_dc_buf,
                NonNull::from(&add),
                NonNull::from(&mask),
                0,
                true,
            )
        }
        .expect("failed to increment DEPART counter");
        qp.scq().poll_one_blocking_consumed();
    }
}

impl<'a> RmaRwLockGuard<'a> {
    pub fn release(self, qp: &Qp, dci: &Qp, dct: &Dct) -> RmaRwLock<'a> {
        if self.shared {
            self.release_sh(qp);
        } else {
            self.release_ex(qp, dci, dct);
        }
        self.lock
    }
}
