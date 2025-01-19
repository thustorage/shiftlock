//! [SIGMOD'18] Distributed Lock Management with RDMA: Decentralization without Starvation.

use std::mem;
use std::ptr::NonNull;

use bitvec::{field::BitField, prelude as bv};
use quanta::Instant;
use rrddmma::{prelude::*, rdma::qp::ExtCompareSwapParams};

use crate::utils::{timing::TimeItem, *};
use crate::LEASE_TIME;

/// DSLR data structure.
///
/// | Field  | LSB | MSB | Description        |
/// | ------ | --: | --: | ------------------ |
/// | SMax   |   0 |  32 | Shared max.        |
/// | XMax   |  32 |  64 | Exclusive max.     |
/// | SCnt   |  64 |  96 | Shared counter.    |
/// | XCnt   |  96 | 128 | Exclusive counter. |
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct DslrEntry(bv::BitArr!(for 128));

impl_lock_basic_methods!(DslrEntry, 128);

impl DslrEntry {
    /// ExtFAA mask.
    pub const MASK: u64 = 0x8000_0000_8000_0000;

    define_field_accessor!(smax, u32, 0..32);
    define_field_accessor!(xmax, u32, 32..64);
    define_field_accessor!(scnt, u32, 64..96);
    define_field_accessor!(xcnt, u32, 96..128);
}

impl std::fmt::Debug for DslrEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DslrEntry")
            .field("smax", &self.smax())
            .field("xmax", &self.xmax())
            .field("scnt", &self.scnt())
            .field("xcnt", &self.xcnt())
            .finish()
    }
}

/// DSLR lock acquire policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DslrAcquirePolicy {
    /// Busy poll without stopping.
    BusyPoll,

    /// Wait for a fixed amount of time for each client before me.
    DynamicWait { nanos: u64 },
}

/// Remote DSLR instance.
#[derive(Debug)]
pub struct DslrLock<'a> {
    local: MrSlice<'a>,
    remote: MrRemote,
}

/// An acquired DSLR instance.
#[derive(Debug)]
pub struct DslrLockGuard<'a> {
    lock: DslrLock<'a>,
    shared: bool,
}

impl DslrLock<'_> {
    fn recover(&self, qp: &Qp, from: DslrEntry, to: DslrEntry) {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.local.addr() as *mut u128) = 0 };

        let mask = ptr_to(&[!0, !0]);
        let compare = NonNull::new(&from as *const DslrEntry as *mut u64).unwrap();
        let swap = NonNull::new(&to as *const DslrEntry as *mut u64).unwrap();
        let params = ExtCompareSwapParams {
            compare,
            swap,
            compare_mask: mask,
            swap_mask: mask,
        };

        // SAFETY: pointers are valid.
        unsafe { qp.ext_compare_swap::<16>(self.local, self.remote, params, 0, true) }.unwrap();
        qp.scq().poll_one_blocking_consumed();
        unsafe { fix_endian::<16>(self.local.addr()) };

        // Recovery can succeed or not, but in both cases we must retry.
        // let entry = unsafe { *DslrEntry::from_addr(self.local.addr()) };
        // if entry.as_u128() == from.as_u128() {
        //     eprintln!("recovery successful");
        // }
    }
}

const OVERRUN_THRESHOLD: u32 = u32::MAX / 2;

impl<'a> DslrLock<'a> {
    /// Create a pointer to a remote lock entry that is not acquired.
    pub fn new(local: MrSlice<'a>, remote: MrRemote) -> Self {
        debug_assert_eq!(local.len(), mem::size_of::<DslrEntry>());
        debug_assert!(local.addr() as usize % mem::align_of::<DslrEntry>() == 0);
        debug_assert_eq!(remote.len, mem::size_of::<DslrEntry>());
        debug_assert!(remote.addr as usize % mem::align_of::<DslrEntry>() == 0);
        Self { local, remote }
    }

    /// Acquire an exclusive lock with the given policy.
    /// This method blocks until the lock is successfully acquired.
    #[allow(clippy::identity_op)]
    pub async fn acquire_ex(self, qp: &Qp, policy: DslrAcquirePolicy) -> Option<DslrLockGuard<'a>> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.local.addr() as *mut u128) = 0 };

        let mask = ptr_to(&[DslrEntry::MASK, DslrEntry::MASK]);
        let add = ptr_to(&[1 << 32, 0]);

        // Issue a fetch-add on the exclusive max.
        // SAFETY: pointers are valid.
        timing::begin_time_op(TimeItem::AcqInitialCasOrWriterFaa);
        unsafe { qp.ext_fetch_add::<16>(self.local, self.remote, add, mask, 0, true) }.unwrap();
        qp.scq().poll_one_blocking_consumed();
        unsafe { fix_endian::<16>(self.local.addr()) };
        timing::end_time_op(TimeItem::AcqInitialCasOrWriterFaa);

        // Read the lock.
        // SAFETY: the address in `local` is guaranteed to be valid.
        let mut entry = *unsafe { DslrEntry::from_addr(self.local.addr()) };
        let (ex_tkt, sh_tkt) = (entry.xmax(), entry.smax());
        let (mut last_entry, mut last_change_time) = (entry, Instant::now());

        timing::begin_time_op(TimeItem::AcqWaitWritersOrRetry);
        while entry.xcnt() != ex_tkt || entry.scnt() != sh_tkt {
            // If overrun, fail retry.
            if ex_tkt.wrapping_sub(entry.xcnt()) > OVERRUN_THRESHOLD
                || sh_tkt.wrapping_sub(entry.scnt()) > OVERRUN_THRESHOLD
            {
                // eprintln!("overrun: {:?}", entry);
                timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                return None;
            }

            if let DslrAcquirePolicy::DynamicWait { nanos } = policy {
                // SAFETY: the address in `local` is guaranteed to be valid.
                let num_waits = ex_tkt.wrapping_sub(entry.xcnt()) as u64
                    + sh_tkt.wrapping_sub(entry.scnt()) as u64;
                async_busy_wait(nanos * num_waits).await;
            }

            // Fetch the newest lock state.
            qp.read(&[self.local], &self.remote, 0, true).unwrap();
            qp.scq().poll_one_blocking_consumed();
            entry = *unsafe { DslrEntry::from_addr(self.local.addr()) };

            if entry.xcnt() != last_entry.xcnt() && entry.scnt() != last_entry.scnt() {
                last_entry = entry;
                last_change_time = Instant::now();
            } else if last_change_time.elapsed() > 2 * LEASE_TIME {
                timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                let mut to = entry;
                to.set_scnt(sh_tkt);
                to.set_xcnt(ex_tkt + 1);

                // eprintln!("recover: {:?} -> {:?}", entry, to);
                self.recover(qp, entry, to);

                return None;
            }
        }
        timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);

        Some(DslrLockGuard {
            lock: self,
            shared: false,
        })
    }

    /// Acquire an exclusive lock with the given policy.
    /// This method blocks until the lock is successfully acquired.
    #[allow(clippy::identity_op)]
    pub async fn acquire_sh(self, qp: &Qp, policy: DslrAcquirePolicy) -> Option<DslrLockGuard<'a>> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.local.addr() as *mut u128) = 0 };

        let mask = ptr_to(&[DslrEntry::MASK, DslrEntry::MASK]);
        let add = ptr_to(&[1, 0]);

        // Issue a fetch-add on the exclusive max.
        // SAFETY: pointers are valid.
        timing::begin_time_op(TimeItem::AcqInitialReaderFaa);
        unsafe { qp.ext_fetch_add::<16>(self.local, self.remote, add, mask, 0, true) }.unwrap();
        qp.scq().poll_one_blocking_consumed();
        unsafe { fix_endian::<16>(self.local.addr()) };
        timing::end_time_op(TimeItem::AcqInitialReaderFaa);

        // Read the lock.
        // SAFETY: the address in `local` is guaranteed to be valid.
        let mut entry = *unsafe { DslrEntry::from_addr(self.local.addr()) };
        let (ex_tkt, sh_tkt) = (entry.xmax(), entry.smax());
        let (mut last_entry, mut last_change_time) = (entry, Instant::now());

        // if ex_tkt.wrapping_sub(entry.xcnt()) > OVERRUN_THRESHOLD
        //     || sh_tkt.wrapping_sub(entry.scnt()) > OVERRUN_THRESHOLD
        // {
        //     let mut to = entry;
        //     to.set_xmax(entry.xcnt());
        //     to.set_smax(entry.scnt());
        //     self.recover(qp, entry, to);
        //     return None;
        // }

        timing::begin_time_op(TimeItem::AcqWaitReadersOrBackoff);
        while entry.xcnt() != ex_tkt {
            // If overrun, fail retry.
            if ex_tkt.wrapping_sub(entry.xcnt()) > OVERRUN_THRESHOLD
                || sh_tkt.wrapping_sub(entry.scnt()) > OVERRUN_THRESHOLD
            {
                // eprintln!("overrun: {:?}", entry);
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
                return None;
            }

            if let DslrAcquirePolicy::DynamicWait { nanos } = policy {
                // SAFETY: the address in `local` is guaranteed to be valid.
                let num_waits = ex_tkt.wrapping_sub(entry.xcnt());
                async_busy_wait(nanos * num_waits as u64).await;
            }

            // Fetch the newest lock state.
            qp.read(&[self.local], &self.remote, 0, true).unwrap();
            qp.scq().poll_one_blocking_consumed();
            entry = *unsafe { DslrEntry::from_addr(self.local.addr()) };

            if entry.xcnt() != last_entry.xcnt() && entry.scnt() != last_entry.scnt() {
                last_entry = entry;
                last_change_time = Instant::now();
            } else if last_change_time.elapsed() > 2 * LEASE_TIME {
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
                let mut to = entry;
                to.set_scnt(sh_tkt + 1);
                to.set_xcnt(ex_tkt);

                // eprintln!("recover: {:?} -> {:?}", entry, to);
                self.recover(qp, entry, to);

                return None;
            }
        }
        timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);

        Some(DslrLockGuard {
            lock: self,
            shared: true,
        })
    }
}

impl<'a> DslrLockGuard<'a> {
    /// Release the lock.
    #[allow(clippy::identity_op)]
    pub fn release(self, qp: &Qp) -> DslrLock<'a> {
        let mask = ptr_to(&[DslrEntry::MASK, DslrEntry::MASK]);
        let add = if self.shared {
            ptr_to(&[0, 1])
        } else {
            ptr_to(&[0, 1 << 32])
        };

        // Issue a fetch-add on the exclusive counter.
        // SAFETY: pointers are valid.
        timing::begin_time_op(TimeItem::RelInitialAtomic);
        unsafe { qp.ext_fetch_add::<16>(self.lock.local, self.lock.remote, add, mask, 0, true) }
            .unwrap();
        qp.scq().poll_one_blocking_consumed();
        timing::end_time_op(TimeItem::RelInitialAtomic);

        self.lock
    }
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
