//! Trivial CAS lock implementation.

use std::ptr::NonNull;
use std::{mem, ptr};

use rand::prelude::*;
use rrddmma::prelude::*;
use rrddmma::rdma::qp::ExtCompareSwapParams;

use crate::utils::{timing::TimeItem, *};
use crate::{ACQUIRED, UNACQUIRED};

/// CAS lock acquire backoff manager.
/// ASPLOS'24 SMART -- Section 4.3.
#[derive(Debug, Clone)]
pub struct Backoff {
    retry_cnt: u32,
    task_cnt: u32,
    initial_backoff_nanos: u64,
    max_backoff_nanos: u64,
    max_backoff_nanos_bound: u64,
}

/// CAS lock wait policy.
#[derive(Debug, Clone)]
pub enum CasWaitPolicy {
    /// Busy poll without stopping.
    BusyPoll,

    /// Truncated exponential backoff.
    /// ASPLOS'24 SMART -- Section 4.3.
    TruncExpBackoff(Backoff),
}

/// CAS lock retry policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasRetryPolicy {
    /// Repeated CAS.
    Cas,
}

/// CAS lock acquire policy.
#[derive(Debug, Clone)]
pub struct CasAcquirePolicy {
    pub wait: CasWaitPolicy,
    pub retry: CasRetryPolicy,
}

impl CasAcquirePolicy {
    pub fn new(wait: CasWaitPolicy, retry: CasRetryPolicy) -> Self {
        CasAcquirePolicy { wait, retry }
    }
}

const BACKOFF_INCREASE_BOUND: i32 = 10;

impl Backoff {
    pub const RETRY_RATE_LOW_WATER_MARK: f32 = 0.1;
    pub const RETRY_RATE_HIGH_WATER_MARK: f32 = 0.5;

    /// Create a new backoff manager.
    pub fn new(initial: u64) -> Self {
        Backoff {
            retry_cnt: 0,
            task_cnt: 0,
            initial_backoff_nanos: initial,
            max_backoff_nanos: initial,
            max_backoff_nanos_bound: initial << BACKOFF_INCREASE_BOUND,
        }
    }

    /// Record a retry event.
    pub fn record_retry(&mut self) {
        self.retry_cnt += 1;
    }

    /// Record a completion event.
    pub fn record_completion(&mut self) {
        self.task_cnt += 1;
    }

    /// Update the backoff time.
    pub fn update(&mut self) {
        if self.retry_cnt + self.task_cnt == 0 {
            return;
        }
        let retry_rate = self.retry_cnt as f32 / (self.retry_cnt + self.task_cnt) as f32;
        if retry_rate < Self::RETRY_RATE_LOW_WATER_MARK {
            self.max_backoff_nanos = self.initial_backoff_nanos.max(self.max_backoff_nanos / 2);
        } else if retry_rate > Self::RETRY_RATE_HIGH_WATER_MARK {
            self.max_backoff_nanos = self.max_backoff_nanos_bound.min(self.max_backoff_nanos * 2);
        }

        self.retry_cnt = 0;
        self.task_cnt = 0;
    }

    /// Get the maximum backoff time.
    pub fn get(&self) -> u64 {
        self.max_backoff_nanos
    }
}

/// Remote CAS lock instance.
pub struct CasLock<'a, const STATE: u8> {
    local: MrSlice<'a>,
    remote: MrRemote,
}

trait PrivateInto<'a, const STATE: u8> {
    fn into_state(self) -> CasLock<'a, STATE>;
}

macro_rules! impl_state_trans {
    ($FROM:expr, $TO:expr) => {
        impl<'a> PrivateInto<'a, $TO> for CasLock<'a, $FROM> {
            fn into_state(self) -> CasLock<'a, $TO> {
                CasLock {
                    local: self.local,
                    remote: self.remote,
                }
            }
        }
    };
}

impl_state_trans!(UNACQUIRED, ACQUIRED);
impl_state_trans!(ACQUIRED, UNACQUIRED);

impl<'a> CasLock<'a, UNACQUIRED> {
    /// Create a pointer to a remote lock entry that is not acquired.
    pub fn new(local: MrSlice<'a>, remote: MrRemote) -> Self {
        debug_assert_eq!(local.len(), mem::size_of::<u64>());
        debug_assert!(local.addr() as usize % mem::align_of::<u64>() == 0);
        debug_assert_eq!(remote.len, mem::size_of::<u64>());
        debug_assert!(remote.addr as usize % mem::align_of::<u64>() == 0);
        CasLock { local, remote }
    }

    /// Try acquiring the lock.
    pub fn try_acquire(self, qp: &Qp, time: bool) -> Result<CasLock<'a, ACQUIRED>, Self> {
        // Issue a compare-swap on the lock entry.
        let compare = 0;
        let swap = 1;
        let mask = !0;
        let params = ExtCompareSwapParams {
            compare: NonNull::from(&compare),
            swap: NonNull::from(&swap),
            compare_mask: NonNull::from(&mask),
            swap_mask: NonNull::from(&mask),
        };

        if time {
            timing::begin_time_op(TimeItem::AcqInitialCasOrWriterFaa);
        }
        unsafe { qp.ext_compare_swap::<8>(self.local, self.remote, params, 0, true) }.unwrap();
        qp.scq().poll_one_blocking_consumed();
        if time {
            timing::end_time_op(TimeItem::AcqInitialCasOrWriterFaa);
        }

        // Check if the lock was acquired.
        // SAFETY: the address in `local` is guaranteed to be valid.
        let lock = unsafe { *(self.local.addr() as *mut u64) };
        if lock == 0 {
            Ok(self.into_state())
        } else {
            Err(self)
        }
    }

    /// Acquire the lock with the given policy.
    pub async fn acquire(
        mut self,
        qp: &Qp,
        policy: &mut CasAcquirePolicy,
    ) -> CasLock<'a, ACQUIRED> {
        match self.try_acquire(qp, true) {
            Ok(acquired) => {
                if let CasWaitPolicy::TruncExpBackoff(ref mut backoff) = policy.wait {
                    backoff.record_completion();
                }
                timing::zero_time_op(TimeItem::AcqWaitWritersOrRetry);
                timing::zero_time_op(TimeItem::AcqWaitReadersOrBackoff);
                return acquired;
            }
            Err(unacquired) => self = unacquired,
        }

        timing::begin_time_op(TimeItem::AcqWaitWritersOrRetry);
        for i in 0.. {
            // tokio::task::yield_now().await;
            if let CasWaitPolicy::TruncExpBackoff(ref mut backoff) = policy.wait {
                if i == 0 {
                    timing::begin_time_op(TimeItem::AcqWaitReadersOrBackoff);
                } else {
                    timing::rebegin_time_op(TimeItem::AcqWaitReadersOrBackoff);
                }
                let max_backoff_nanos = backoff.get();
                let nanos = max_backoff_nanos
                    .min(backoff.initial_backoff_nanos << i.min(BACKOFF_INCREASE_BOUND));
                let jitter = thread_rng().gen_range(0..backoff.initial_backoff_nanos);

                backoff.record_retry();
                busy_wait(nanos + jitter);
                timing::end_time_op(TimeItem::AcqWaitReadersOrBackoff);
            }

            match self.try_acquire(qp, false) {
                Ok(acquired) => {
                    if let CasWaitPolicy::TruncExpBackoff(ref mut backoff) = policy.wait {
                        backoff.record_completion();
                    }
                    timing::end_time_op(TimeItem::AcqWaitWritersOrRetry);
                    return acquired;
                }
                Err(unacquired) => self = unacquired,
            }
        }
        unreachable!()
    }
}

impl<'a> CasLock<'a, ACQUIRED> {
    /// Release the lock.
    pub async fn release(self, qp: &Qp) -> CasLock<'a, UNACQUIRED> {
        // SAFETY: the address in `local` is guaranteed to be valid.
        unsafe { ptr::write(self.local.addr() as *mut u64, 0) };

        // Perform unlock.
        timing::begin_time_op(TimeItem::RelInitialAtomic);
        qp.compare_swap(self.local, self.remote, 1, 0, 0, true)
            .unwrap();
        qp.scq().poll_one_blocking_consumed();
        timing::end_time_op(TimeItem::RelInitialAtomic);

        self.into_state()
    }
}
