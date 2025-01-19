//! Distributed MCS lock implementation.

use std::ptr::NonNull;
use std::{fmt, mem, ptr};

use bitvec::{field::BitField, prelude as bv};
use rrddmma::prelude::*;
use rrddmma::rdma::cq::WcStatus;
use rrddmma::rdma::qp::ExtCompareSwapParams;

#[allow(unused)]
use crate::utils::*;
use crate::{ACQUIRED, ACQUIRING, UNACQUIRED};

/// MCS lock data structure.
///
/// | Field  | LSB | MSB | Description           |
/// | ------ | --: | --: | --------------------- |
/// | xlock  |   0 |   3 | Exclusive lock bits.  |
/// | scnt   |   4 |  15 | Shared counter.       |
/// | node   |  16 |  31 | Queue tail node ID.   |
/// | dctnum |  32 |  63 | Queue tail DCT num.   |
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct McsEntry(bv::BitArr!(for 64));

impl_lock_basic_methods!(McsEntry, 64);

impl McsEntry {
    /// Lock occupation bits base.
    pub const XBITS_BASE: u64 = 1;

    /// Shared counter base.
    pub const SCNT_BASE: u64 = 1 << 4;

    /// Queue tail node ID base.
    pub const TAIL_NODEID_BASE: u64 = 1 << 16;

    /// Queue tail DCT num base.
    pub const TAIL_DCTNUM_BASE: u64 = 1 << 32;

    /// Mask without scnt.
    pub const MASK_NO_SCNT: u64 = !0 - (McsEntry::TAIL_NODEID_BASE - McsEntry::SCNT_BASE);
}

impl McsEntry {
    define_field_accessor!(xbits, u8, 0..4);
    define_field_accessor!(scnt, u16, 4..16);
    define_field_accessor!(node, u16, 16..32);
    define_field_accessor!(dctnum, u32, 32..64);
}

impl fmt::Debug for McsEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McsEntry")
            .field("xbits", &self.xbits())
            .field("scnt", &self.scnt())
            .field("node", &self.node())
            .field("dctnum", &self.dctnum())
            .finish()
    }
}

impl fmt::Display for McsEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "McsEntry {:#x}", self.as_u64())
    }
}

/// Notification type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum NotifyType {
    Successor = 1,
    Handover = 2,
}

/// Packed notification data among clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct Notification {
    /// Notification type.
    pub ty: NotifyType,

    /// Node ID of the client.
    pub node: u16,

    /// DCT number of the client.
    pub dctnum: u32,
}

impl Notification {
    pub fn new(ty: NotifyType, node: u16, dctnum: u32) -> Self {
        Self { ty, node, dctnum }
    }
}

/// Remote MCS lock instance.
pub struct McsLock<'a, const STATE: u8> {
    id: u16,
    local: MrSlice<'a>,
    remote: MrRemote,
    next: Option<Notification>,
}

trait PrivateInto<'a, const STATE: u8> {
    fn into_state(self) -> McsLock<'a, STATE>;
}

macro_rules! impl_state_trans {
    ($FROM:expr, $TO:expr) => {
        impl<'a> PrivateInto<'a, $TO> for McsLock<'a, $FROM> {
            fn into_state(self) -> McsLock<'a, $TO> {
                McsLock {
                    id: self.id,
                    local: self.local,
                    remote: self.remote,
                    next: self.next,
                }
            }
        }
    };
}

impl_state_trans!(UNACQUIRED, ACQUIRING);
impl_state_trans!(UNACQUIRED, ACQUIRED);
impl_state_trans!(ACQUIRING, ACQUIRED);

// Separate implementation to clear `next` field.
impl<'a> PrivateInto<'a, UNACQUIRED> for McsLock<'a, ACQUIRED> {
    fn into_state(self) -> McsLock<'a, UNACQUIRED> {
        McsLock {
            id: self.id,
            local: self.local,
            remote: self.remote,
            next: None,
        }
    }
}

const COMPENSATE_NANOS: u64 = 1000;

impl<'a> McsLock<'a, UNACQUIRED> {
    /// Create a new unacquired lock instance.
    pub fn new(id: u16, local: MrSlice<'a>, remote: MrRemote) -> Self {
        debug_assert!(local.len() >= mem::size_of::<McsEntry>() * 4);
        debug_assert!(local.addr() as usize % mem::align_of::<McsEntry>() == 0);
        assert_eq!(
            remote.len,
            mem::size_of::<McsEntry>() + mem::size_of::<u64>()
        );
        debug_assert!(remote.addr as usize % mem::align_of::<McsEntry>() == 0);
        Self {
            id,
            local,
            remote,
            next: None,
        }
    }

    /// Try acquiring the lock.
    ///
    /// Arguments:
    ///
    /// - `qp`: QP to the lock server. Must match the `remote` when creating the lock.
    /// - `dci`: DCI of the current client.
    /// - `dct`: DCT of the current client.
    #[allow(clippy::identity_op)]
    pub fn try_acquire(
        self,
        qp: &Qp,
        dci: &Qp,
        dct: &Dct,
    ) -> Result<McsLock<'a, ACQUIRED>, McsLock<'a, ACQUIRING>> {
        // Issue a fetch-set on the lock entry to update the MCS queue tail.
        const ZERO: u64 = 0;

        let swap = {
            let occupied = McsEntry::XBITS_BASE * 1;
            let dct = McsEntry::TAIL_DCTNUM_BASE * dct.dct_num() as u64;
            let node = McsEntry::TAIL_NODEID_BASE * self.id as u64;
            occupied + dct + node
        };

        let remote_entry = self.remote.slice(0, 8).unwrap();
        {
            let params = ExtCompareSwapParams {
                compare: NonNull::from(&ZERO),
                swap: NonNull::from(&swap),
                compare_mask: NonNull::from(&ZERO),
                swap_mask: NonNull::from(&McsEntry::MASK_NO_SCNT),
            };
            // SAFETY: parameters are valid.
            let local = self.local.slice(0, 8).unwrap();
            unsafe { qp.ext_compare_swap::<8>(local, remote_entry, params, 0, true) }.unwrap();
            qp.scq().poll_one_blocking_consumed();
        }

        // Read the lock.
        // SAFETY: the address in `local` is guaranteed to be valid.
        let lock = *unsafe { McsEntry::from_addr(self.local.addr()) };

        // If there is no one holding the lock, then I get the lock.
        if lock.dctnum() == 0 && lock.node() == 0 {
            return Ok(self.into_state());
        }

        // Tell the previous node that I am acquiring the lock.
        let ep = QpEndpoint::new(None, lock.node(), qp.port().unwrap().0.num(), lock.dctnum());
        let peer = dci.make_peer(ep).unwrap();

        if lock.node() == self.id {
            // Compensate for the intra-node communication.
            busy_wait(COMPENSATE_NANOS);
        }

        // Send packed data to the previous node.
        let not = Notification::new(NotifyType::Successor, self.id, dct.dct_num());
        let local = self.local.slice(0, 8).unwrap();
        // SAFETY: the address in `local` is guaranteed to be valid.
        unsafe { ptr::write(self.local.addr() as *mut Notification, not) };

        dci.send(&[local], Some(&peer), None, 0, true, true)
            .unwrap();
        dci.scq().poll_one_blocking_consumed();

        Err(self.into_state())
    }

    /// Acquire the lock, blocks until success.
    pub async fn acquire(self, qp: &Qp, dci: &Qp, dct: &Dct) -> McsLock<'a, ACQUIRED> {
        let lock = self.try_acquire(qp, dci, dct);
        match lock {
            Ok(lock) => lock,
            Err(unacquired) => unacquired.finish(dct).await,
        }
    }
}

impl<'a> McsLock<'a, ACQUIRING> {
    /// Poll the lock to check if it is acquired.
    pub async fn poll(mut self, dct: &Dct) -> Result<McsLock<'a, ACQUIRED>, Self> {
        // Poll the DCT CQ.
        let mut wc = [Wc::default(); 2];
        let num = dct.cq().poll_into(&mut wc).unwrap();

        let mut ok = false;
        for wc in wc.iter().take(num as _) {
            assert_eq!(wc.status(), WcStatus::Success);
            let id = wc.wr_id();
            // SAFETY: the address in `local` is guaranteed to be valid, and has at least 24B allocation unit.
            let not = unsafe {
                ptr::read_volatile(
                    self.local.addr().add((id as usize + 1) * 8) as *const Notification
                )
            };
            unsafe { ptr::write_bytes(self.local.addr().add((id as usize + 1) * 8), 0, 8) };

            // Replenish the recv buffer.
            let local = self.local.slice((id as usize + 1) * 8, 8).unwrap();
            dct.srq().recv(&[local], id).unwrap();

            match not.ty {
                NotifyType::Successor => {
                    assert!(self.next.is_none());
                    self.next = Some(not);
                }
                NotifyType::Handover => ok = true,
            }
        }

        if ok {
            Ok(self.into_state())
        } else {
            Err(self)
        }
    }

    /// Poll the lock until it is acquired.
    /// This method blocks until the lock is successfully acquired.
    pub async fn finish(mut self, dct: &Dct) -> McsLock<'a, ACQUIRED> {
        loop {
            match self.poll(dct).await {
                Ok(acquired) => return acquired,
                Err(unacquired) => {
                    tokio::task::yield_now().await;
                    self = unacquired;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

impl<'a> McsLock<'a, ACQUIRED> {
    /// Release the lock.
    #[allow(clippy::identity_op)]
    pub async fn release(self, qp: &Qp, dci: &Qp, dct: &Dct) -> McsLock<'a, UNACQUIRED> {
        fn do_notify_next(dci: &Qp, next: Notification, local: MrSlice) {
            assert!(next.dctnum != 0 && next.node != 0);
            let ep = QpEndpoint::new(None, next.node, dci.port().unwrap().0.num(), next.dctnum);
            let peer = dci.make_peer(ep).unwrap();

            // Send packed data to the previous node.
            let not = Notification::new(NotifyType::Handover, 0, 0);
            let local = local.slice(0, 8).unwrap();
            // SAFETY: the address in `local` is guaranteed to be valid.
            unsafe { ptr::write(local.addr() as *mut Notification, not) };

            dci.send(&[local], Some(&peer), None, 0, true, true)
                .unwrap();
            dci.scq().poll_one_blocking_consumed();
        }

        // Zero the buffer just in case.
        // SAFETY: the address in `local` is guaranteed to be valid.
        unsafe { ptr::write(self.local.addr() as *mut u64, 0) };

        // If I have already got the next, simply notify it.
        if let Some(next) = self.next {
            // Increase a RelCnt counter so that the lock has FT ability.
            let remote_relcnt = self.remote.slice(8, 8).unwrap();
            qp.fetch_add(
                self.local.slice(24, 32).unwrap(),
                remote_relcnt,
                1,
                0,
                false,
            )
            .unwrap();

            do_notify_next(dci, next, self.local);
        } else {
            // Issue a compare-and-swap to release the lock.
            let compare = {
                let occupied = McsEntry::XBITS_BASE * 1;
                let dct = McsEntry::TAIL_DCTNUM_BASE * dct.dct_num() as u64;
                let node = McsEntry::TAIL_NODEID_BASE * self.id as u64;
                occupied + dct + node
            };
            assert_eq!(compare & McsEntry::MASK_NO_SCNT, compare);

            let remote_entry = self.remote.slice(0, 8).unwrap();
            const ZERO: u64 = 0;
            {
                let params = ExtCompareSwapParams {
                    compare: NonNull::from(&compare),
                    swap: NonNull::from(&ZERO),
                    compare_mask: NonNull::from(&McsEntry::MASK_NO_SCNT),
                    swap_mask: NonNull::from(&McsEntry::MASK_NO_SCNT),
                };
                // SAFETY: parameters are valid.
                let local = self.local.slice(0, 8).unwrap();
                unsafe { qp.ext_compare_swap::<8>(local, remote_entry, params, 0, true) }.unwrap();
                qp.scq().poll_one_blocking_consumed();
            }

            // Read the lock.
            // SAFETY: the address in `local` is guaranteed to be valid.
            let lock = unsafe { ptr::read(self.local.addr() as *const McsEntry) };

            // If cmpxchg succeeds, then I have released the lock.
            if lock.dctnum() == dct.dct_num() && lock.node() == self.id {
                return self.into_state();
            }

            // Otherwise, wait for the next node and notify it.
            // Poll the DCT CQ.
            let wc = dct.cq().poll_one_blocking().unwrap();
            let id = wc.wr_id();

            let not =
                unsafe { *(self.local.addr().add((id as usize + 1) * 8) as *const Notification) };

            // Replenish the recv buffer.
            let local = self.local.slice((id as usize + 1) * 8, 8).unwrap();
            dct.srq().recv(&[local], id).unwrap();

            assert_eq!(not.ty, NotifyType::Successor);

            // Increase a RelCnt counter so that the lock has FT ability.
            let remote_relcnt = self.remote.slice(8, 8).unwrap();
            qp.fetch_add(
                self.local.slice(24, 32).unwrap(),
                remote_relcnt,
                1,
                0,
                false,
            )
            .unwrap();
            do_notify_next(dci, not, self.local);
        }
        self.into_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout() {
        use std::mem::{align_of, size_of};
        assert_eq!(size_of::<McsEntry>(), 8);
        assert_eq!(align_of::<McsEntry>(), 8);

        assert_eq!(size_of::<Notification>(), 8);
    }
}
