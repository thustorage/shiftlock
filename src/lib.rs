mod macros;

pub mod app;
pub mod baselines;
mod shiftlock;
pub mod utils;

pub use shiftlock::*;
pub use utils::*;

pub const NTHREADS: usize = 8;
pub const DURATION: std::time::Duration = std::time::Duration::from_secs(10);

/// The lock lease time.
#[cfg(feature = "recovery")]
pub const LEASE_TIME: std::time::Duration = std::time::Duration::from_millis(10);

#[cfg(not(feature = "recovery"))]
pub const LEASE_TIME: std::time::Duration = std::time::Duration::from_millis(1000);

pub const EXTRA_COUNT: u64 = 10;

pub const LOCK_RELEASE: u64 = 0;
pub const LOCK_ACQUIRE_EX: u64 = 1;
pub const LOCK_ACQUIRE_SH: u64 = 2;
pub const LOCK_RECOVER: u64 = 999999;
