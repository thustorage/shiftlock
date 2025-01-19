//! Handlock implementation.

use std::ptr::NonNull;
use std::{fmt, hint, mem, ptr};

use bitvec::{field::BitField, prelude as bv};
use quanta::Instant;
use rrddmma::prelude::*;
use rrddmma::rdma::qp::ExtCompareSwapParams;
use thiserror::Error;

use crate::utils::{timing::TimeItem, *};
use crate::{LEASE_TIME, LOCK_RECOVER};

mod ahcache;
use ahcache::NodeDct;

pub mod consrec;

mod ft;

#[allow(unused)]
static DEBUG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Handlock data structure.
///
/// | Field     | LSB | Len | Description                     |
/// | --------- | --: | --: | ------------------------------- |
/// | mode      |   0 |   1 | Lock mode.                      |
/// | recover   |   1 |   1 | Whether the lock is recovering. |
/// | rdr_cnt   |   2 |  13 | Reader count.                   |
/// | tail_node |  16 |  16 | Queue tail node ID.             |
/// | tail_dct  |  32 |  32 | Queue tail DCT num.             |
/// | rel_cnt   |  64 |  64 | Lock release count.             |
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(align(16))]
pub struct HandlockEntry(bv::BitArr!(for 128));

impl_lock_basic_methods!(HandlockEntry, 128);

impl HandlockEntry {
    define_field_accessor!(mode, u8, 0..1, WITH_MASK);
    define_field_accessor!(rdr_cnt, u32, 1..24, WITH_MASK);
    define_field_accessor!(tail_node, u16, 24..40, WITH_MASK);
    define_field_accessor!(tail_dct, u32, 40..64, WITH_MASK);

    define_field_accessor!(major, u64, 0..64);
    define_field_accessor!(rel_cnt, u64, 64..128);
}

const ENTIRE_BYTES: usize = mem::size_of::<HandlockEntry>();
const HALF_BYTES: usize = ENTIRE_BYTES / 2;

const NOTI_BUF_OFFSET: usize = ENTIRE_BYTES + mem::size_of::<Notification>() * 2;

/// When there are this many consecutive writers acquiring the lock, the current client
/// should cooperatively try to handle the lock over to readers.
#[cfg(not(feature = "custcwt"))]
pub const MODE_CHANGE_THRESHOLD: u32 = 16;

#[cfg(feature = "custcwt")]
pub const MODE_CHANGE_THRESHOLD: u32 = {
    let thres_str = std::option_env!("HANDLOCK_CONSWRT_THRESHOLD");
    match thres_str {
        Some(s) => {
            let mut thres = 0;
            let mut i = 0;
            while i < s.len() {
                thres = thres * 10 + (s.as_bytes()[i] - b'0') as u32;
                i += 1;
            }
            thres
        }
        None => 16,
    }
};

impl fmt::Debug for HandlockEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlockEntry")
            .field("mode", &self.mode())
            .field("rdr_cnt", &self.rdr_cnt())
            .field("tail_node", &self.tail_node())
            .field("tail_dct", &self.tail_dct())
            .field("rel_cnt", &self.rel_cnt())
            .finish()
    }
}

impl fmt::Display for HandlockEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HandlockEntry(major={:#016x}, rel_cnt={:#016x})",
            self.major(),
            self.rel_cnt()
        )
    }
}

/// Location of era.
use std::sync::atomic::{AtomicU64, Ordering};
static ERA_LOC: AtomicU64 = AtomicU64::new(0);

/// Remote Handlock instance.
#[derive(Debug)]
pub struct Handlock<'a> {
    /// Local node ID. Currently we use the LID.
    id: u16,

    /// Local RDMA-capable memory buffer.
    buf: MrSlice<'a>,

    /// Remote pointer to the lock entry.
    remote: MrRemote,
}

/// Lock acquisition policy.
#[derive(Debug, Clone, Copy)]
pub struct HandlockAcquirePolicy {
    /// Whether to wait when a remote poll returns NotReady.
    /// Unit: nanos.
    pub remote_wait: Option<u64>,
}

/// Lock type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockType {
    Exclusive {
        /// Consecutive writer count.
        cons_wrt_cnt: u32,

        /// Lock release count at the moment of acquisition.
        pred_rel_cnt: u64,

        /// Information of the successor client.
        succ: Option<NodeDct>,

        /// Acquire Time.
        acquire_time: u64,
    },
    Shared,
}

/// An acquired Handlock instance.
#[derive(Debug)]
pub struct HandlockGuard<'a> {
    /// The original lock.
    lock: Handlock<'a>,

    /// Lock type.
    ty: LockType,

    /// Current lock mode.
    mode: u8,
}

impl HandlockGuard<'_> {
    /// Return `true` if this is an exclusive lock guard.
    #[inline(always)]
    pub fn is_exclusive(&self) -> bool {
        matches!(self.ty, LockType::Exclusive { .. })
    }
}

#[derive(Debug, Error)]
enum LockError {
    /// Lease expiration: `RelCnt` has not changed for twice the lease time.
    #[error("lease expired with RelCnt={0}")]
    LeaseExpired(u64),

    /// Need to retry because lock was reset by some other client, or just I
    /// waited for too long between two adjacent checks.
    #[error("lock was reset or wait for too long")]
    Retry,
}

/// Blocking methods of HandLock should return this type as they must handle client failures.
/// i.e., failure recovery.
type WaitResult<T> = Result<T, LockError>;

/// RelCnt leap threshold. Observing such a leap definitely means that the lock has been reset.
pub const RELCNT_LEAP: u64 = 1 << 63;

/// Nanoseconds to busy wait when sending a notification locally.
const COMPENSATE_NANOS: u64 = 1000;

/// Return the current timestamp in nanoseconds.
pub fn current_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("failed to get current time")
        .as_nanos() as u64
}

impl<'a> Handlock<'a> {
    /// Poll the DCT SRQ for notifications.
    /// Return an optional notification from predecessor and an optional notification from successor.
    #[inline(always)]
    fn poll_dct(&self, dct: &Dct) -> (Option<(Notification, u64)>, Option<NodeDct>) {
        let mut pred = None;
        let mut succ = None;

        let mut wc = [ExpWc::default(); 2];
        let n = dct
            .cq()
            .exp_poll_into(&mut wc)
            .expect("failed to poll DCT CQ");

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
                Notification::Successor(suc) => {
                    debug_assert!(succ.is_none());
                    succ = Some(suc);
                }
                _ => {
                    debug_assert!(pred.is_none());
                    let ts = wc.timestamp().unwrap_or(0);
                    pred = Some((noti, ts));
                }
            };
        }
        (pred, succ)
    }

    /// Blockingly wait for readers to release the reader lock, until the lock release count reaches
    /// the expected value. During the poll, also check the DCT SRQ for notification from the
    /// successor, and return it if any.
    ///
    /// # Caveats
    ///
    /// - This method overwrites the `rel_cnt` bytes (8..16) of the local buffer.
    /// - This method assumes that it won't receive any notification from the predecessor.
    fn wait_for_readers(
        &self,
        qp: &Qp,
        dct: &Dct,
        expected: u64,
        mut prev_rel_cnt: u64,
        policy: HandlockAcquirePolicy,
    ) -> WaitResult<Option<NodeDct>> {
        timing::begin_time_op(TimeItem::AcqWaitReadersOrBackoff);

        // Successor notification buffer.
        let mut successor = None;

        let local_rel_cnt_buf = self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
        let remote_rel_cnt_buf = self
            .remote
            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
            .unwrap();
        let mut last_change_time = Instant::now();
        let mut last_check_time = last_change_time;
        loop {
            // Read the `rel_cnt` field of the lock.
            let cur_check_time = Instant::now();
            qp.read(&[local_rel_cnt_buf], &remote_rel_cnt_buf, 0, true)
                .expect("failed to read rel_cnt");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            let cur_rel_cnt = unsafe { ptr::read(local_rel_cnt_buf.addr() as *const u64) };

            // If all readers have left, I should stop waiting.
            let units = expected.wrapping_sub(cur_rel_cnt);
            if units == 0 {
                break;
            }

            // Check for `RelCnt` leap here. If there is a leap, then the lock must have been reset.
            if units >= RELCNT_LEAP {
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
                return Err(LockError::Retry);
            }

            // Also check DCT SRQ here, because a successor writer may appear.
            let (pred, succ) = self.poll_dct(dct);
            debug_assert!(pred.is_none());
            debug_assert!(
                successor.and(succ).is_none(),
                "Notification from successor appeared twice"
            );
            successor = successor.or(succ);

            // Wait for a period of time as required by the policy.
            if let Some(wait_dur) = policy.remote_wait {
                busy_wait(wait_dur);
            }

            // Check for `RelCnt` changes. If it keeps unchanged for a long time, we must be observing a client failure.
            let cur_time = Instant::now();
            if cur_rel_cnt != prev_rel_cnt {
                last_change_time = cur_time;
                prev_rel_cnt = cur_rel_cnt;
            } else if cur_time - last_change_time > LEASE_TIME * 3 {
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
                return Err(LockError::LeaseExpired(cur_rel_cnt));
            }

            // The time between two adjacent checks must not be longer than lease time, or the judgments can be incorrect.
            // If this happens, I should retry.
            if cur_time - last_check_time > LEASE_TIME {
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
                return Err(LockError::Retry);
            }
            last_check_time = cur_check_time;
        }

        timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
        Ok(successor)
    }

    /// Reset the lock for failure recovery.
    ///
    /// It is expected at the time of invocation of this method, there are no clients releasing the lock,
    /// i.e., `RelCnt` cannot be changed by any clients except those wishing to reset the lock.
    /// Therefore, even if I failed to reset the lock (by CAS), someone must have already done it.
    #[cfg(feature = "legacy_recovery")]
    fn reset(&self, qp: &Qp, cur_rel_cnt: u64) {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };

        // Issue a CAS to reset the lock.
        // This operation:
        // - Flips the MSB of `rel_cnt` (i.e., add 2^63).
        // - Clears the major part of the lock.
        let new_rel_cnt = cur_rel_cnt ^ RELCNT_LEAP;
        let compare_mask = [0, !0];
        let compare = [0, cur_rel_cnt];
        let swap_mask = [!0, !0];
        let swap = [0, new_rel_cnt];

        let local_rd_buf = self.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();
        let cas_params = ExtCompareSwapParams {
            compare: ptr_to(&compare),
            swap: ptr_to(&swap),
            compare_mask: ptr_to(&compare_mask),
            swap_mask: ptr_to(&swap_mask),
        };
        // SAFETY: pointers are all from references and are therefore valid.
        unsafe {
            qp.ext_compare_swap::<ENTIRE_BYTES>(local_rd_buf, self.remote, cas_params, 0, true)
        }
        .expect("failed to perform Reset ExtCmpSwp");
        qp.scq().poll_one_blocking_consumed();

        // Verification: if I failed, then someone must have successed.
        let entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };
        if entry.rel_cnt() != cur_rel_cnt {
            debug_assert!(entry.rel_cnt().wrapping_sub(cur_rel_cnt) >= RELCNT_LEAP);
        }
    }

    #[cfg(not(feature = "legacy_recovery"))]
    fn reset(&self, qp: &Qp, cur_rel_cnt: u64) {
        let local_buf = self.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();
        while !self.request_reset(qp) {
            busy_wait_dur(LEASE_TIME);

            // Zero the local buffer just in case.
            // SAFETY: buffer pointer validity verified during creation.
            unsafe { *(self.buf.addr() as *mut u128) = 0 };

            // Read the lock entry.
            qp.read(&[local_buf], &self.remote, 0, true)
                .expect("failed to read lock entry");
            qp.scq().poll_one_blocking_consumed();

            let entry = unsafe { ptr::read(self.buf.addr() as *const HandlockEntry) };
            if entry.rel_cnt() != cur_rel_cnt {
                break;
            }
        }
    }

    /// Request the server to reset the lock.
    pub fn request_reset(&self, qp: &Qp) -> bool {
        // Zero the local buffer just in case.
        let offset = ENTIRE_BYTES + 3 * mem::size_of::<Notification>();
        unsafe { ptr::write_bytes(self.buf.addr().add(offset), 0, 4 * size_of::<u64>()) };

        // Read the current era.
        // SAFETY: buffer pointer validity verified during creation.
        let era_buf = self
            .buf
            .slice(offset + 3 * size_of::<u64>(), size_of::<u64>())
            .unwrap();
        let remote_era = MrRemote {
            addr: ERA_LOC.load(Ordering::Relaxed),
            len: mem::size_of::<u64>(),
            rkey: self.remote.rkey,
        };
        qp.read(&[era_buf], &remote_era, 0, true)
            .expect("failed to read era");
        qp.scq().poll_one_blocking_consumed();
        let era = unsafe { ptr::read(era_buf.addr() as *const u64) };

        // Zero the local buffer just in case.
        unsafe { ptr::write_bytes(self.buf.addr().add(offset), 0, 4 * size_of::<u64>()) };

        // Post recv.
        let recv_buf = era_buf;
        qp.recv(&[recv_buf], 0)
            .expect("failed to post reset request recv");

        // Write RPC request.
        let send_buf = self.buf.slice(offset, 3 * size_of::<u64>()).unwrap();
        unsafe {
            ptr::write(send_buf.addr() as *mut u64, LOCK_RECOVER);
            ptr::write((send_buf.addr() as *mut u64).add(1), self.remote.addr);
            ptr::write((send_buf.addr() as *mut u64).add(2), era);
        }

        // Post send.
        qp.send(&[send_buf], None, None, 0, true, true)
            .expect("failed to post reset request send");
        qp.scq().poll_one_blocking_consumed();

        // Wait for the response.
        qp.rcq().poll_one_blocking_consumed();

        // Read the response.
        // SAFETY: buffer pointer validity verified during creation.
        let resp = unsafe { ptr::read(recv_buf.addr() as *const u64) };
        return resp == 1;
    }

    /// Trivially try to acquire an exclusive lock with original CAS and a retry count bound.
    /// This may improve performance when only a few clients (2~3) contend for the lock.
    #[allow(unused)]
    fn acquire_ex_trivial(
        self,
        qp: &Qp,
        dct: &Dct,
        policy: HandlockAcquirePolicy,
    ) -> Result<HandlockGuard<'a>, Self> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };
        let local_rd_buf = self.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();

        let mask = [mask_of!(HandlockEntry: rdr_cnt, tail_node, tail_dct), 0];
        let compare = [0, 0];
        let swap = [
            bit_repr_of!(HandlockEntry: {
                tail_node: self.id,
                tail_dct: dct.dct_num(),
            }),
            0,
        ];
        let cas_params = ExtCompareSwapParams {
            compare: ptr_to(&compare),
            swap: ptr_to(&swap),
            compare_mask: ptr_to(&mask),
            swap_mask: ptr_to(&mask),
        };

        const RETRY_LIMIT: u32 = 2;
        for i in 0..RETRY_LIMIT {
            // SAFETY: pointers are all from references and are therefore valid.
            unsafe {
                qp.ext_compare_swap::<ENTIRE_BYTES>(local_rd_buf, self.remote, cas_params, 0, true)
            }
            .expect("failed to perform acquire_ex_trivial ExtCmpSwp");
            qp.scq().poll_one_blocking_consumed();

            // Check if the CAS was successful. If yes, wait for readers and acquire the lock.
            // SAFETY: buffer pointer validity verified during creation.
            unsafe { fix_endian::<ENTIRE_BYTES>(local_rd_buf.addr()) };
            let entry = unsafe { ptr::read(self.buf.addr() as *const HandlockEntry) };

            if entry.tail_node() == 0 && entry.tail_dct() == 0 && entry.rdr_cnt() == 0 {
                return Ok(HandlockGuard {
                    lock: self,
                    ty: LockType::Exclusive {
                        cons_wrt_cnt: 0,
                        pred_rel_cnt: entry.rel_cnt(),
                        succ: None,
                        acquire_time: current_nanos(),
                    },
                    mode: entry.mode(),
                });
            }

            if i + 1 < RETRY_LIMIT {
                busy_wait(policy.remote_wait.unwrap_or(0) << i);
            }
        }
        Err(self)
    }
}

impl<'a> Handlock<'a> {
    /// Create a new unacquired lock instance.
    pub fn new(id: u16, buf: MrSlice<'a>, remote: MrRemote) -> Self {
        // Remote lock pointer validity.
        debug_assert!(remote.addr % mem::align_of::<HandlockEntry>() as u64 == 0);
        debug_assert_eq!(remote.len, ENTIRE_BYTES);

        // Local buffer validity.
        debug_assert!(buf.addr() as usize % 8 == 0);
        debug_assert!(
            buf.len()
                >= ENTIRE_BYTES + 3 * mem::size_of::<Notification>() + 4 * mem::size_of::<u64>()
        );

        Self { id, buf, remote }
    }

    /// Initiate the DCT SRQ with `recv`s.
    /// The user must call this function EXACTLY ONCE initially before any lock acquisition.
    pub fn initiate_dct(dct: &Dct, buf: MrSlice) -> std::io::Result<()> {
        assert!(buf.addr() as usize % 8 == 0);
        assert!(buf.len() >= ENTIRE_BYTES + 3 * mem::size_of::<Notification>());

        for _ in 0..2 {
            for i in 0..2 {
                let recv_buf = buf
                    .slice(
                        ENTIRE_BYTES + i * mem::size_of::<Notification>(),
                        mem::size_of::<Notification>(),
                    )
                    .unwrap();
                dct.srq().recv(&[recv_buf], i as _)?;
            }
        }
        Ok(())
    }

    /// Initiate the era location.
    /// The user must call this function once before any lock acquisition.
    pub fn initiate_era(era_loc: u64) {
        ERA_LOC.store(era_loc, Ordering::SeqCst);
    }

    /// Acquire an exlusive lock with the given policy.
    ///
    /// # Arguments
    ///
    /// - `qp`: QP to the lock server that contains the lock entry.
    /// - `dci`: DCI QP of the current client.
    /// - `dct`: DCT of the current client.
    #[allow(unused_mut)]
    pub async fn acquire_ex(
        mut self,
        qp: &Qp,
        dci: &mut Qp,
        dct: &Dct,
        policy: HandlockAcquirePolicy,
    ) -> Option<HandlockGuard<'a>> {
        // Before all, try to trivially acquire the lock to improve some performance when contention
        // is low but not very low.
        self = match self.acquire_ex_trivial(qp, dct, policy) {
            Ok(guard) => return Some(guard),
            Err(lock) => lock,
        };

        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };

        // Issue an ExtCmpSwp to the lock entry, adding myself to the end of the MCS queue.
        // This operation:
        // - Overwrites `tail_node` and `tail_dct` unconditionally with those of myself;
        //   the ExtCmpSwp will return the previous values for me.
        // - Does not touch `mode`, `rdr_cnt`, and `rel_cnt`; these value will determine whether and how I
        //   should synchronize with the concurrent readers.
        let compare_and_mask = [0, 0];
        let swap_mask = [mask_of!(HandlockEntry: tail_node, tail_dct), 0];
        let swap = [
            bit_repr_of!(HandlockEntry: {
                tail_node: self.id,
                tail_dct: dct.dct_num(),
            }),
            0,
        ];

        let local_rd_buf = self.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();
        let cas_params = ExtCompareSwapParams {
            compare: ptr_to(&compare_and_mask),
            swap: ptr_to(&swap),
            compare_mask: ptr_to(&compare_and_mask),
            swap_mask: ptr_to(&swap_mask),
        };

        let acquire_time = current_nanos();
        timing::begin_time_op(TimeItem::AcqInitialCasOrWriterFaa);
        // SAFETY: pointers are all from references and are therefore valid.
        unsafe {
            qp.ext_compare_swap::<ENTIRE_BYTES>(local_rd_buf, self.remote, cas_params, 0, true)
        }
        .expect("failed to perform initial ExtCmpSwp");
        qp.scq().poll_one_blocking_consumed();
        timing::end_time_op(TimeItem::AcqInitialCasOrWriterFaa);

        // SAFETY: buffer pointer validity verified during creation.
        unsafe { fix_endian::<ENTIRE_BYTES>(local_rd_buf.addr()) };
        let mut entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };

        // A successor may appear when I am waiting for the lock.
        let mut succ = None;

        // Now I have added myself to the MCS queue and got my predecessor, and there are three cases to handle:
        // 1. If the predecessor is not null, then I in most cases will receive the lock handed over from him.
        //    In this case, I must first send a `Successor` notification to him, and then wait for his `Handover`
        //    or `ModeChanged` notification.
        //    There is no need to check for readers *now*, as when I receive the lock, all these things should already
        //    have been handled by my predecessors.
        if entry.tail_dct() != 0 {
            // Sanity check: predecessor is not self.
            debug_assert!(entry.tail_node() != self.id || entry.tail_dct() != dct.dct_num());

            // Bind the DCI to the predecessor's DCT (rti = routing info).
            let pred_rti = NodeDct::new(entry.tail_node(), entry.tail_dct());
            if pred_rti.node == self.id {
                busy_wait(COMPENSATE_NANOS);
            }

            let pred_peer = ahcache::get_in_local_cache(pred_rti, dci);

            // Send the `Successor` notification.
            let noti = Notification::Successor(NodeDct::new(self.id, dct.dct_num()));
            let noti_buf = self
                .buf
                .slice(NOTI_BUF_OFFSET, mem::size_of::<Notification>())
                .unwrap();
            // SAFETY: buffer pointer validity verified during creation.
            unsafe { ptr::write(noti_buf.addr() as *mut Notification, noti) };

            timing::begin_time_op(TimeItem::AcqNotify);
            dci.send(&[noti_buf], Some(&pred_peer), None, 0, true, true)
                .expect("failed to send to predecessor");
            if let Ok(wc) = dci.scq().poll_one_blocking() {
                if wc.status() != WcStatus::Success {
                    let (port, gid_index) = dci.port().unwrap().clone();
                    dci.reset().unwrap();
                    dci.bind_local_port(&port, Some(gid_index)).unwrap();
                }
            }
            timing::end_time_op(TimeItem::AcqNotify);

            // Wait locally for notification from the previous writer.
            let mut last_change_time = Instant::now();
            let mut last_check_time = last_change_time;
            let mut prev_rel_cnt = entry.rel_cnt();

            const TIME_EVERY: usize = 128;
            let mut time_count = 0usize;

            timing::begin_time_op(TimeItem::AcqWaitPred);
            let (expected_mode, expected_rel_cnt) = loop {
                let (pred, succ2) = self.poll_dct(dct);
                debug_assert!(
                    succ.and(succ2).is_none(),
                    "received duplicate Successor notifications"
                );
                succ = succ.or(succ2);

                if let Some((pred, acquire_time)) = pred {
                    timing::end_time_op(TimeItem::AcqWaitPred);
                    match pred {
                        // Direct lock handover, I can enter my critical section.
                        Notification::Handover {
                            mode,
                            cons_wrt_cnt,
                            pred_rel_cnt,
                        } => {
                            // Sanity check: handed-over lock can never have a zero cons_wrt_cnt.
                            debug_assert!(cons_wrt_cnt != 0);
                            timing::zero_time_op(TimeItem::AcqWaitReadersOrBackoff);

                            return Some(HandlockGuard {
                                lock: self,
                                ty: LockType::Exclusive {
                                    cons_wrt_cnt,
                                    pred_rel_cnt,
                                    succ,
                                    acquire_time,
                                },
                                mode,
                            });
                        }

                        // The lock was handed over to readers, I should spin on the lock entry again.
                        Notification::ModeChanged {
                            expected_mode,
                            expected_rel_cnt,
                        } => {
                            break (expected_mode, expected_rel_cnt);
                        }

                        _ => {
                            // Safety in debug mode, squeeze some performance in release mode.
                            if cfg!(debug_assertions) {
                                unreachable!()
                            } else {
                                // SAFETY: the implementation of `self.poll_dct()` guarantees that `pred` cannot
                                // be `Successor`.
                                unsafe { hint::unreachable_unchecked() }
                            }
                        }
                    }
                }

                // Check for lease expiration. Get current time per `TIME_EVERY` loops to save timing overheads.
                time_count += 1;
                if time_count == TIME_EVERY {
                    time_count = 0;

                    // Poll `RelCnt` every half the lease time.
                    let cur_time = Instant::now();
                    if cur_time - last_check_time >= LEASE_TIME / 2 {
                        // Read the `rel_cnt` field of the lock.
                        let local_rel_cnt_buf =
                            self.buf.slice_by_range(HALF_BYTES..ENTIRE_BYTES).unwrap();
                        let remote_rel_cnt_buf = self
                            .remote
                            .slice_by_range(HALF_BYTES..ENTIRE_BYTES)
                            .unwrap();

                        timing::begin_time_op(TimeItem::AcqCheckFailure);
                        let cur_check_time = Instant::now();
                        qp.read(&[local_rel_cnt_buf], &remote_rel_cnt_buf, 0, true)
                            .expect("failed to read rel_cnt");
                        qp.scq().poll_one_blocking_consumed();
                        timing::end_time_op(TimeItem::AcqCheckFailure);

                        // The time between two adjacent checks must not be longer than lease time, or the judgments can be incorrect.
                        // If this happens, I should retry.
                        if last_check_time.elapsed() > LEASE_TIME {
                            timing::end_time_op(TimeItem::AcqWaitPred);
                            return None;
                        }

                        // Update the read value to `entry`.
                        // SAFETY: buffer pointer validity verified during creation.
                        entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };

                        // Check for `RelCnt` change. If it keeps unchanged for a long time, we must be observing a client failure.
                        if entry.rel_cnt() != prev_rel_cnt {
                            // Check for `RelCnt` leap. If there is a leap, then the lock must have been reset, and I need to retry.
                            if entry.rel_cnt().wrapping_sub(prev_rel_cnt) >= RELCNT_LEAP {
                                timing::end_time_op(TimeItem::AcqWaitPred);
                                return None;
                            }
                            last_change_time = cur_time;
                            prev_rel_cnt = entry.rel_cnt();
                        } else if cur_time - last_change_time > LEASE_TIME * 3 {
                            timing::end_time_op(TimeItem::AcqWaitPred);
                            self.reset(qp, entry.rel_cnt());
                            return None;
                        }

                        last_check_time = cur_check_time;
                    }
                }
            };

            // Here, what I got from the previous writer must be `ModeChanged`, since we would have already returned
            // if we got a `Handover`. The expected release count is stored in `exp_rel_cnt`.
            if entry.rel_cnt() != expected_rel_cnt {
                let succ2 =
                    match self.wait_for_readers(qp, dct, expected_rel_cnt, entry.rel_cnt(), policy)
                    {
                        Ok(s) => s,
                        Err(LockError::Retry) => {
                            // The lock was reset by some other client. I should retry.
                            return None;
                        }
                        Err(LockError::LeaseExpired(cur_rel_cnt)) => {
                            // The lease has expired. I should reset the lock.
                            self.reset(qp, cur_rel_cnt);
                            return None;
                        }
                    };
                debug_assert!(
                    succ.and(succ2).is_none(),
                    "received duplicate Successor notifications"
                );
                succ = succ.or(succ2);
            }

            // Fallback to the codepath of Situation 3.
            // SAFETY: buffer pointer validity verified during creation.
            entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };
            debug_assert_eq!(entry.rel_cnt(), expected_rel_cnt);

            // Since the mode has changed, we must flip our mode.
            entry.set_mode(expected_mode);
        }
        // 2. If the predecessor is null but `rdr_cnt` is not zero, then I must wait for all readers to leave.
        //    The signal for this is when `lock.rel_cnt` becomes `current_rel_cnt + rdr_cnt`, so I need to spin on the
        //    lock entry's `rel_cnt` field to detect this.
        else if entry.rdr_cnt() != 0 {
            timing::zero_time_op(TimeItem::AcqNotify);
            timing::zero_time_op(TimeItem::AcqWaitPred);

            let expected_rel_cnt = entry.rel_cnt().wrapping_add(entry.rdr_cnt() as u64);
            succ = match self.wait_for_readers(qp, dct, expected_rel_cnt, entry.rel_cnt(), policy) {
                Ok(s) => s,
                Err(LockError::Retry) => {
                    // The lock was reset by some other client. I should retry.
                    return None;
                }
                Err(LockError::LeaseExpired(cur_rel_cnt)) => {
                    // The lease has expired. I should reset the lock.
                    self.reset(qp, cur_rel_cnt);
                    return None;
                }
            };

            // Upon exit of the loop above, all readers entered the lock before me should have released the lock.
            // I can now safely acquire the lock.
            // SAFETY: buffer pointer validity verified during creation.
            entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };
            debug_assert_eq!(entry.rel_cnt(), expected_rel_cnt);

            // Fallback to the codepath of Situation 3.
        } else {
            timing::zero_time_op(TimeItem::AcqNotify);
            timing::zero_time_op(TimeItem::AcqWaitPred);
            timing::zero_time_op(TimeItem::AcqWaitReadersOrBackoff);
        }

        // 3. If the predecessor is null and `rdr_cnt` is zero, then I am the only client currently trying to acquire
        //    the lock. I can happily enter my critical section.
        Some(HandlockGuard {
            lock: self,
            ty: LockType::Exclusive {
                cons_wrt_cnt: 0,
                pred_rel_cnt: entry.rel_cnt(),
                succ,
                acquire_time,
            },
            mode: entry.mode(),
        })
    }

    /// Acquire a shared lock.
    ///
    /// # Arguments
    ///
    /// - `qp`: QP to the lock server that contains the lock entry.
    pub async fn acquire_sh(
        self,
        qp: &Qp,
        policy: HandlockAcquirePolicy,
    ) -> Option<HandlockGuard<'a>> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u128) = 0 };

        // Issue an ExtFetchAdd to the lock entry, increasing the reader count.
        let local_buf = self.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();
        let add_mask = [mask_of!(FAA, HandlockEntry: rdr_cnt), 0];
        let add = [bit_repr_of!(HandlockEntry: { rdr_cnt: 1u64 }), 0];

        timing::begin_time_op(TimeItem::AcqInitialReaderFaa);
        // SAFETY: pointers are all from references and are therefore valid.
        unsafe {
            qp.ext_fetch_add::<ENTIRE_BYTES>(
                local_buf,
                self.remote,
                ptr_to(&add),
                ptr_to(&add_mask),
                0,
                true,
            )
        }
        .expect("failed to perform AcquireSh ExtFetchAdd");
        qp.scq().poll_one_blocking_consumed();
        timing::end_time_op(TimeItem::AcqInitialReaderFaa);

        // SAFETY: buffer pointer validity verified during creation.
        unsafe { fix_endian::<ENTIRE_BYTES>(local_buf.addr()) };
        let mut entry = unsafe { ptr::read(self.buf.addr() as *const HandlockEntry) };

        // If the writer MCS queue is empty, I can just directly acquire the lock.
        if entry.tail_node() == 0 && entry.tail_dct() == 0 {
            timing::zero_time_op(TimeItem::AcqWaitWritersOrRetry);
            return Some(HandlockGuard {
                lock: self,
                ty: LockType::Shared,
                mode: entry.mode(),
            });
        }

        // Wait until the lock mode changes.
        timing::begin_time_op(TimeItem::AcqWaitWritersOrRetry);
        let expected_mode = entry.mode() ^ 1;
        let mut last_change_time = Instant::now();
        let mut last_check_time = last_change_time;
        let mut prev_rel_cnt = entry.rel_cnt();
        loop {
            // Wait for a period of time as required by the policy.
            if let Some(wait_dur) = policy.remote_wait {
                busy_wait(wait_dur);
            }

            // Read the lock entry again.
            let cur_check_time = Instant::now();
            qp.read(&[local_buf], &self.remote, 0, true)
                .expect("failed to read major");
            qp.scq().poll_one_blocking_consumed();

            // Check if the lock mode has changed.
            // SAFETY: buffer pointer validity verified during creation.
            entry = unsafe { ptr::read(self.buf.addr() as *const HandlockEntry) };
            if entry.mode() == expected_mode {
                timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                return Some(HandlockGuard {
                    lock: self,
                    ty: LockType::Shared,
                    mode: entry.mode(),
                });
            }

            // Check for `RelCnt` change. If it keeps unchanged for a long time, we must be observing a client failure.
            let cur_time = Instant::now();
            if cur_time - last_check_time > LEASE_TIME {
                timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                return None;
            }

            if entry.rel_cnt() != prev_rel_cnt {
                // Check for `RelCnt` leap. If there is a leap, then the lock must have been reset, and I need to retry.
                if entry.rel_cnt().wrapping_sub(prev_rel_cnt) >= RELCNT_LEAP {
                    timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                    return None;
                }
                last_change_time = cur_time;
                prev_rel_cnt = entry.rel_cnt();
            } else if cur_time - last_change_time > LEASE_TIME * 3 {
                timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                self.reset(qp, entry.rel_cnt());
                return None;
            }
            last_check_time = cur_check_time;
        }
    }
}

impl<'a> HandlockGuard<'a> {
    /// Send a notification message to the successor writer.
    ///
    /// # Caveat
    ///
    /// - This method overwrites the `major` bytes (0..8) of the local buffer.
    fn notify_succ(&self, dci: &mut Qp, succ: NodeDct, noti: Notification) {
        timing::begin_time_op(TimeItem::RelNotify);
        // SAFETY: buffer pointer validity verified during creation.
        let noti_buf = self
            .lock
            .buf
            .slice(NOTI_BUF_OFFSET, mem::size_of::<Notification>())
            .unwrap();
        unsafe { ptr::write(noti_buf.addr() as *mut Notification, noti) };

        let peer = ahcache::get_in_local_cache(succ, dci);
        dci.send(&[noti_buf], Some(&peer), None, 1, true, true)
            .expect("failed to send to successor");
        if let Ok(wc) = dci.scq().poll_one_blocking() {
            if wc.status() != WcStatus::Success {
                let (port, gid_index) = dci.port().unwrap().clone();
                dci.reset().unwrap();
                dci.bind_local_port(&port, Some(gid_index)).unwrap();
            }
        }
        timing::end_time_op(TimeItem::RelNotify);
    }

    /// Release an exclusive lock.
    fn release_ex(&self, qp: &Qp, dci: &mut Qp, dct: &Dct) {
        // Sanity check.
        let LockType::Exclusive {
            cons_wrt_cnt,
            pred_rel_cnt,
            succ,
            ..
        } = self.ty
        else {
            panic!("release_ex called on a shared lock");
        };
        let cons_wrt_cnt = cons_wrt_cnt.saturating_add(1);

        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.lock.buf.addr() as *mut u128) = 0 };
        let local_rd_buf = self.lock.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();

        // If the successor is not present, I assume it does not exist and try to CAS the MCS queue tail to null.
        timing::begin_time_op(TimeItem::RelInitialAtomic);
        if succ.is_none() {
            timing::zero_time_op(TimeItem::RelTransferAtomic);

            // Issue a ExtCmpSwp to the lock entry's major part.
            // This operation:
            // - Tries to zero `tail_node` and `tail_dct`.
            // - Flips the `mode` to enable pending readers to enter the lock.
            let compare_mask = [mask_of!(HandlockEntry: tail_node, tail_dct), 0];
            let swap_mask = [mask_of!(HandlockEntry: mode, tail_node, tail_dct), !0];
            let compare = [
                bit_repr_of!(HandlockEntry: {
                    tail_node: self.lock.id,
                    tail_dct: dct.dct_num(),
                }),
                0,
            ];
            let swap = [
                bit_repr_of!(HandlockEntry: { mode: self.mode ^ 1 }),
                pred_rel_cnt.wrapping_add(1),
            ];

            let cas_params = ExtCompareSwapParams {
                compare: ptr_to(&compare),
                swap: ptr_to(&swap),
                compare_mask: ptr_to(&compare_mask),
                swap_mask: ptr_to(&swap_mask),
            };

            // SAFETY: pointers are all from references and are therefore valid.
            unsafe {
                qp.ext_compare_swap::<ENTIRE_BYTES>(
                    local_rd_buf,
                    self.lock.remote,
                    cas_params,
                    1,
                    true,
                )
            }
            .expect("failed to perform situ.1 ExtCmpSwp in release");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            unsafe { fix_endian::<ENTIRE_BYTES>(local_rd_buf.addr()) };
            let entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };

            // If the CAS succeeded, then there is no successor writer, and I can return.
            if entry.tail_node() == self.lock.id && entry.tail_dct() == dct.dct_num() {
                timing::end_time_op(TimeItem::RelInitialAtomic);
                timing::zero_time_op(TimeItem::RelNotify);
                timing::zero_time_op(TimeItem::RelWaitSucc);

                consrec::cons_wrt_rec(cons_wrt_cnt);

                return;
            }
        }
        timing::end_time_op(TimeItem::RelInitialAtomic);

        // Determine who I should pass the lock to.
        // Criteria: consecutive writer count.
        if cons_wrt_cnt < MODE_CHANGE_THRESHOLD {
            // Prefer passing the lock to the successor writer.
            let succ = match succ {
                Some(s) => {
                    timing::zero_time_op(TimeItem::RelWaitSucc);
                    s
                }
                None => {
                    timing::begin_time_op(TimeItem::RelWaitSucc);
                    let start = Instant::now();
                    loop {
                        let (_, succ) = self.lock.poll_dct(dct);
                        if let Some(succ) = succ {
                            timing::end_time_op(TimeItem::RelWaitSucc);
                            break succ;
                        }
                        if start.elapsed() >= LEASE_TIME {
                            timing::end_time_op(TimeItem::RelWaitSucc);
                            return;
                        }
                        hint::spin_loop();
                    }
                }
            };

            // Handover the lock.
            let noti = Notification::Handover {
                mode: self.mode,
                cons_wrt_cnt,
                pred_rel_cnt,
            };
            self.notify_succ(dci, succ, noti);
            consrec::handover_rec();
        } else {
            // Prefer passing the lock to readers.
            //
            // With the presence of a successor writer, I should separately update the RelCnt.
            // Also, I need to send a `ModeChanged` notification to the successor.
            // These operations *cannot* be parallelized as the notification needs the reader count returned by the CAS.
            //
            // Perform the CAS to flip the `mode` and increment the release count.
            // SAFETY: pointers are all from references and are therefore valid.
            let compare_mask = [mask_of!(HandlockEntry: mode), 0];
            let compare = [bit_repr_of!(HandlockEntry: { mode: self.mode }), 0];
            let swap_mask = [mask_of!(HandlockEntry: mode), !0];
            let swap = [
                bit_repr_of!(HandlockEntry: { mode: self.mode ^ 1 }),
                pred_rel_cnt.wrapping_add(1),
            ];

            let cas_params = ExtCompareSwapParams {
                compare: ptr_to(&compare),
                swap: ptr_to(&swap),
                compare_mask: ptr_to(&compare_mask),
                swap_mask: ptr_to(&swap_mask),
            };

            timing::begin_time_op(TimeItem::RelTransferAtomic);
            unsafe {
                qp.ext_compare_swap::<ENTIRE_BYTES>(
                    local_rd_buf,
                    self.lock.remote,
                    cas_params,
                    0,
                    true,
                )
            }
            .expect("failed to perform situ.2 ExtCAS in release");
            qp.scq().poll_one_blocking_consumed();
            timing::end_time_op(TimeItem::RelTransferAtomic);

            // SAFETY: buffer pointer validity verified during creation.
            unsafe { fix_endian::<ENTIRE_BYTES>(local_rd_buf.addr()) };
            let entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };

            // Interrupted by a recovery, retry.
            if entry.mode() != self.mode {
                return;
            }

            // Get the routing information of the successor writer.
            // If the successor is not present, I should wait for him to notify me.
            let succ = match succ {
                Some(s) => {
                    timing::zero_time_op(TimeItem::RelWaitSucc);
                    s
                }
                None => {
                    timing::begin_time_op(TimeItem::RelWaitSucc);
                    let start = Instant::now();
                    loop {
                        let (_, succ) = self.lock.poll_dct(dct);
                        if let Some(succ) = succ {
                            timing::end_time_op(TimeItem::RelWaitSucc);
                            break succ;
                        }
                        if start.elapsed() >= LEASE_TIME {
                            timing::end_time_op(TimeItem::RelWaitSucc);
                            return;
                        }
                        hint::spin_loop();
                    }
                }
            };

            // Notify the successor of a lock mode change.
            let noti = Notification::ModeChanged {
                expected_mode: self.mode ^ 1,
                expected_rel_cnt: pred_rel_cnt.wrapping_add(entry.rdr_cnt() as u64 + 1),
            };
            self.notify_succ(dci, succ, noti);

            consrec::cons_wrt_rec(cons_wrt_cnt);
        }
    }

    fn release_sh(&self, qp: &Qp) {
        // Sanity check.
        if !matches!(self.ty, LockType::Shared) {
            panic!("release_sh called on an exclusive lock");
        }

        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.lock.buf.addr() as *mut u128) = 0 };

        // Issue an ExtFetchAdd to the lock entry, decreasing the reader count and increasing the release count.
        let local_rd_buf = self.lock.buf.slice_by_range(0..ENTIRE_BYTES).unwrap();
        let add_mask = [mask_of!(FAA, HandlockEntry: rdr_cnt), 0];
        let add = [mask_of!(HandlockEntry: rdr_cnt), 1];

        timing::begin_time_op(TimeItem::RelInitialAtomic);
        // SAFETY: pointers are all from references and are therefore valid.
        unsafe {
            qp.ext_fetch_add::<ENTIRE_BYTES>(
                local_rd_buf,
                self.lock.remote,
                ptr_to(&add),
                ptr_to(&add_mask),
                0,
                true,
            )
        }
        .expect("fail to perform ExtFetchAdd in release_sh");
        qp.scq().poll_one_blocking_consumed();
        timing::end_time_op(TimeItem::RelInitialAtomic);

        // SAFETY: buffer pointer validity verified during creation.
        unsafe { fix_endian::<ENTIRE_BYTES>(local_rd_buf.addr()) };
        let entry = unsafe { ptr::read(local_rd_buf.addr() as *const HandlockEntry) };

        debug_assert_eq!(entry.mode(), self.mode);
    }
}

impl<'a> HandlockGuard<'a> {
    /// Release the lock and return the original lock instance.
    pub fn release(self, qp: &Qp, dci: &mut Qp, dct: &Dct) -> Handlock<'a> {
        match self.ty {
            LockType::Shared => self.release_sh(qp),
            _ => self.release_ex(qp, dci, dct),
        }
        self.lock
    }
}

/// Notification type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Notification {
    /// Exclusive-lock handover from the previous client.
    Handover {
        /// Current lock mode. Useful when handover the lock to readers.
        mode: u8,

        /// Consecutive writer count.
        cons_wrt_cnt: u32,

        /// Lock release count at the moment of handover.
        pred_rel_cnt: u64,
    },

    /// Previous client released the exclusive lock, but it was
    /// handed over to readers. I should spin on the lock entry again.
    ModeChanged {
        /// Expected mode.
        expected_mode: u8,

        /// Expected lock release count for the successor writer.
        /// The successor can only acquire the lock when the lock release count
        /// reaches this value.
        expected_rel_cnt: u64,
    },

    /// Successor client entered the queue.
    /// When I release my lock, I should notice him with `Handover` or
    /// `ModeChanged` depending on some current status.
    Successor(NodeDct),
}

/// Generate a `NonNull<u64>` from a reference to a `[u64; 2]` for use in ExtAtomics.
fn ptr_to(val: &[u64; 2]) -> NonNull<u64> {
    NonNull::from(&val[0])
}

/// Fix the endian of ExtAtomics response.
#[inline(always)]
unsafe fn fix_endian<const LEN: usize>(buf: *mut u8) {
    if LEN <= 8 {
        return;
    }

    if cfg!(target_endian = "little") {
        for i in 0..LEN / 8 {
            let ptr = unsafe { buf.add(i * 8) as *mut u64 };
            let val = unsafe { *ptr };
            unsafe { *ptr = val.swap_bytes() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout() {
        use std::mem::{align_of, size_of};
        assert_eq!(size_of::<HandlockEntry>(), 16);
        assert_eq!(align_of::<HandlockEntry>(), 16);
    }

    #[test]
    fn test_masks() {
        assert_eq!(mask_of!(HandlockEntry: mode), 0b1);
        assert_eq!(
            mask_of!(HandlockEntry: tail_node),
            0b1111_1111_1111_1111_0000_0000_0000_0000
        );
        assert_eq!(
            mask_of!(HandlockEntry: mode, tail_node),
            0b1111_1111_1111_1111_0000_0000_0000_0001
        );
        assert_eq!(
            mask_of!(FAA, HandlockEntry: tail_node),
            0b1000_0000_0000_0000_0000_0000_0000_0000
        );

        assert_eq!(bit_repr_of!(HandlockEntry: { mode: 1u64 }), 0b1);
        assert_eq!(
            bit_repr_of!(HandlockEntry: { tail_node: 0x1234u64 }),
            0x1234 << 16
        );
    }
}
