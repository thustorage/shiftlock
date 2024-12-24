//! High-precision wait.

use std::hint::spin_loop;
use std::time::Duration;

use quanta::Instant;

/// Synchronously busy-wait for some nanoseconds.
pub fn busy_wait(nanos: u64) {
    if nanos == 0 {
        return;
    }
    let start = Instant::now();
    let end = start + Duration::from_nanos(nanos);
    while Instant::now() < end {
        // Hint the CPU to spin.
        spin_loop();
    }
}

/// Synchronously busy-wait for some duration.
pub fn busy_wait_dur(dur: Duration) {
    let start = Instant::now();
    let end = start + dur;
    while Instant::now() < end {
        // Hint the CPU to spin.
        spin_loop();
    }
}

/// Asynchronously busy-wait for some nanoseconds.
pub async fn async_busy_wait(nanos: u64) {
    if nanos == 0 {
        return;
    }
    let start = std::time::Instant::now();
    let end = start + Duration::from_nanos(nanos);

    const NANOS_PER_MILLI: u64 = 1_000_000;
    if nanos >= NANOS_PER_MILLI {
        tokio::time::sleep(Duration::from_millis(nanos / NANOS_PER_MILLI)).await;
    }
    while std::time::Instant::now() < end {
        tokio::task::yield_now().await;
    }
}

/// Asynchronously busy-wait for some duration.
pub async fn async_busy_wait_dur(dur: Duration) {
    async_busy_wait(dur.as_nanos() as u64).await;
}
