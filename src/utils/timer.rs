//! Time the lock operations.

use quanta::Instant;
use std::time::Duration;

#[derive(Clone)]
pub struct TimerStats {
    /// CDF of latencies.
    pub cdf: [Duration; 100],

    /// Average latency.
    pub avg: Duration,

    /// The number of samples.
    pub total: usize,
}

impl std::fmt::Debug for TimerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timer")
            .field("total", &self.total)
            .field("lat-p50", &self.median())
            .field("lat-p99", &self.p99())
            .finish()
    }
}

impl TimerStats {
    /// Get the median latency.
    pub fn median(&self) -> Duration {
        self.cdf[50]
    }

    /// Get the 99th percentile latency.
    pub fn p99(&self) -> Duration {
        self.cdf[99]
    }

    /// Write the results to a text file.
    pub fn append_to_file(&self, path: &str, secs: f64) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::options().append(true).create(true).open(path)?;
        for (i, lat) in self.cdf.iter().enumerate() {
            writeln!(file, "{},{}", i, lat.as_nanos())?;
        }
        writeln!(file, "total,{},{:.1}", self.total, self.total as f64 / secs)?;
        writeln!(file, "")?;
        Ok(())
    }
}

/// Thread-local timer.
pub struct Timer {
    /// A vector of timed latencies.
    latencies: Vec<Duration>,
}

impl Timer {
    /// Create a timer.
    pub const fn new() -> Self {
        Self {
            latencies: Vec::new(),
        }
    }

    /// Return true if the timer is empty.
    pub fn is_empty(&self) -> bool {
        self.latencies.is_empty()
    }

    /// Append entry.
    pub fn push(&mut self, latency: Duration) {
        self.latencies.push(latency);
    }

    /// Make the last entry of the timer be merged into its previous entry.
    /// If there are fewer than two entries, this is a no-op.
    pub fn digest_last(&mut self) {
        if self.latencies.len() >= 2 {
            let last = self.latencies.pop().unwrap();
            let prev = self.latencies.last_mut().unwrap();
            *prev += last;
        }
    }

    /// Merge a timer's data into this timer.
    pub fn merge(&mut self, other: Timer) {
        self.latencies.extend(other.latencies);
    }

    /// Merge a timer's data into this timer, but does not drop the other timer.
    /// This will cause the other timer to be empty.
    pub fn merge_mut(&mut self, other: &mut Timer) {
        self.latencies.extend(other.latencies.drain(..));
    }

    /// Report statistics.
    pub fn report(&mut self) -> TimerStats {
        // Find each i-th percentile of the latencies.
        let total = self.latencies.len();
        if total == 0 {
            return TimerStats {
                cdf: [Duration::ZERO; 100],
                avg: Duration::ZERO,
                total: 0,
            };
        }
        self.latencies.sort_unstable();

        let avg = self.latencies.iter().sum::<Duration>() / total as u32;
        let mut cdf = [Duration::ZERO; 100];
        for (i, lat) in cdf.iter_mut().enumerate().skip(1) {
            let idx = (total * i) / 100;
            *lat = self.latencies[idx];
        }
        TimerStats { cdf, avg, total }
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

/// An op-guard that maintains timing.
pub struct TimerOp<'a> {
    timer: &'a mut Timer,
    start: Instant,
}

impl<'a> TimerOp<'a> {
    pub fn new(timer: &'a mut Timer) -> Self {
        Self {
            timer,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for TimerOp<'a> {
    fn drop(&mut self) {
        self.timer.latencies.push(self.start.elapsed());
    }
}
