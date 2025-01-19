use rrddmma::prelude::*;
use std::time::Duration;

use crate::utils;

/// Microbenchmark: Read-modify-write a specific part of memory.
pub struct MicroRmw<'a> {
    local: MrSlice<'a>,
    remote: MrRemote,
    think: Duration,
}

impl<'a> MicroRmw<'a> {
    /// Create a new RMW execution instance.
    pub fn new(local: MrSlice<'a>, remote: MrRemote, think: Duration) -> Self {
        assert_eq!(local.len(), remote.len());
        Self {
            local,
            remote,
            think,
        }
    }
}

impl super::Application for MicroRmw<'_> {
    /// Run the workload once.
    fn run(&self, qp: &Qp) {
        qp.read(&[self.local], &self.remote, 0, true)
            .expect("failed to perform RMW.read");
        qp.scq().poll_one_blocking_consumed();

        utils::busy_wait(self.think.as_nanos() as u64);

        qp.write(&[self.local], &self.remote, 1, None, true)
            .expect("failed to perform RMW.write");
        qp.scq().poll_one_blocking_consumed();
    }
}

/// Microbenchmark: Read a specific part of memory.
pub struct MicroRo<'a> {
    local: MrSlice<'a>,
    remote: MrRemote,
}

impl<'a> MicroRo<'a> {
    /// Create a new RMW execution instance.
    pub fn new(local: MrSlice<'a>, remote: MrRemote) -> Self {
        assert_eq!(local.len(), remote.len());
        Self { local, remote }
    }
}

impl super::Application for MicroRo<'_> {
    /// Run the workload once.
    fn run(&self, qp: &Qp) {
        qp.read(&[self.local], &self.remote, 0, true)
            .expect("failed to perform RO.read");
        qp.scq().poll_one_blocking_consumed();
    }
}

/// Microbenchmark: Write a specific part of memory.
pub struct MicroWo<'a> {
    local: MrSlice<'a>,
    remote: MrRemote,
}

impl<'a> MicroWo<'a> {
    /// Create a new RMW execution instance.
    pub fn new(local: MrSlice<'a>, remote: MrRemote) -> Self {
        assert_eq!(local.len(), remote.len());
        Self { local, remote }
    }
}

impl super::Application for MicroWo<'_> {
    /// Run the workload once.
    fn run(&self, qp: &Qp) {
        qp.write(&[self.local], &self.remote, 1, None, true)
            .expect("failed to perform WO.write");
        qp.scq().poll_one_blocking_consumed();
    }
}
