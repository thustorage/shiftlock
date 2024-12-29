//! DrTM: write CAS + read timestamp-based lease.

use std::{mem, ptr};

use bitvec::{field::BitField, prelude as bv};
use rrddmma::prelude::*;

pub mod time {
    use lazy_static::lazy_static;
    use libc::*;

    pub struct PtpTimeSource {
        fd: c_int,
        clkid: c_int,
    }

    impl PtpTimeSource {
        pub fn new() -> Self {
            Self {
                fd: -1,
                clkid: CLOCK_REALTIME,
            }
        }

        pub fn get(&self) -> timespec {
            let mut ts = timespec {
                tv_nsec: 0,
                tv_sec: 0,
            };
            unsafe {
                let mut ret = clock_gettime(self.clkid, &mut ts);
                while ret != 0 {
                    ret = clock_gettime(self.clkid, &mut ts);
                }
            }
            ts
        }
    }

    impl Default for PtpTimeSource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Drop for PtpTimeSource {
        fn drop(&mut self) {
            if self.fd != -1 {
                unsafe {
                    close(self.fd);
                }
            }
        }
    }

    lazy_static! {
        static ref PTP: PtpTimeSource = PtpTimeSource::new();
    }

    pub const DELTA: u64 = 50_000;
    pub const RLEASE_TIME: u64 = 1_000_000;
    pub const TX_TIME: u64 = 500_000;

    pub fn now() -> u64 {
        let ts = PTP.get();
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }

    pub fn valid(ts: u64) -> bool {
        now() < ts - DELTA - TX_TIME
    }

    pub fn expired(ts: u64) -> bool {
        now() > ts + DELTA
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DrtmEntry(bv::BitArr!(for 64));

impl_lock_basic_methods!(DrtmEntry, 64);

impl DrtmEntry {
    define_field_accessor!(write_lock, u8, 0..1, WITH_MASK);
    define_field_accessor!(owner_id, u8, 1..9, WITH_MASK);
    define_field_accessor!(read_lease, u64, 9..64, WITH_MASK);
}

#[derive(Debug)]
pub struct DrtmLock<'a> {
    id: u8,
    buf: MrSlice<'a>,
    remote: MrRemote,
}

#[derive(Debug)]
pub struct DrtmLockGuard<'a> {
    /// The original lock.
    lock: DrtmLock<'a>,

    /// Lock type.
    shared: bool,

    /// Read lease timestamp.
    end_time: u64,
}

fn state_locked(id: u8) -> u64 {
    bit_repr_of!(DrtmEntry: {
        write_lock: 1u64,
        owner_id: id,
    })
}

fn state_read_leased(end_time: u64) -> u64 {
    bit_repr_of!(DrtmEntry: {
        read_lease: end_time,
    })
}

impl<'a> DrtmLock<'a> {
    pub fn new(id: u8, buf: MrSlice<'a>, remote: MrRemote) -> Self {
        // Remote lock pointer validity.
        debug_assert!(remote.addr % mem::align_of::<DrtmEntry>() as u64 == 0);
        debug_assert_eq!(remote.len, 8);
        debug_assert_eq!(buf.len(), 8);
        Self { id, buf, remote }
    }

    pub async fn acquire_ex(self, qp: &Qp, _: u64) -> DrtmLockGuard<'a> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u64) = 0 };

        let mut cur_state = 0;
        loop {
            qp.compare_swap(
                self.buf,
                self.remote,
                cur_state,
                state_locked(self.id),
                0,
                true,
            )
            .expect("failed to CAS ex");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            let state = unsafe { ptr::read(self.buf.addr() as *const DrtmEntry) };
            if state.as_u64() == cur_state {
                return DrtmLockGuard {
                    lock: self,
                    shared: false,
                    end_time: 0,
                };
            } else if state.write_lock() == 0 && time::expired(state.read_lease()) {
                cur_state = state.as_u64();
            }
        }
    }

    pub async fn acquire_sh(self, qp: &Qp, _: u64) -> DrtmLockGuard<'a> {
        // Zero the local buffer just in case.
        // SAFETY: buffer pointer validity verified during creation.
        unsafe { *(self.buf.addr() as *mut u64) = 0 };

        let mut cur_state = 0;
        loop {
            let end_time = time::now() + time::RLEASE_TIME;
            qp.compare_swap(
                self.buf,
                self.remote,
                cur_state,
                state_read_leased(end_time),
                0,
                true,
            )
            .expect("failed to CAS sh");
            qp.scq().poll_one_blocking_consumed();

            // SAFETY: buffer pointer validity verified during creation.
            let state = unsafe { ptr::read(self.buf.addr() as *const DrtmEntry) };
            if state.as_u64() == cur_state {
                return DrtmLockGuard {
                    lock: self,
                    shared: true,
                    end_time,
                };
            } else if state.write_lock() != 0 {
                cur_state = 0;
            } else if time::expired(state.read_lease()) {
                cur_state = state.as_u64();
            } else if time::valid(state.read_lease()) {
                return DrtmLockGuard {
                    lock: self,
                    shared: true,
                    end_time: state.read_lease(),
                };
            }
        }
    }
}

impl<'a> DrtmLockGuard<'a> {
    pub fn valid(&self) -> bool {
        !self.shared || time::valid(self.end_time)
    }

    pub fn release(self, qp: &Qp) -> DrtmLock<'a> {
        if !self.shared {
            qp.compare_swap(
                self.lock.buf,
                self.lock.remote,
                state_locked(self.lock.id),
                0,
                1,
                true,
            )
            .expect("failed to release");
            qp.scq().poll_one_blocking_consumed();
        }
        self.lock
    }
}
