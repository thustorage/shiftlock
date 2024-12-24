//! Application logic (simulated).

mod micro_rmw;
mod trace;

pub use micro_rmw::*;
pub use trace::*;

use rrddmma::rdma::qp::Qp;

/// Trait for types that can simulate application logic.
pub trait Application {
    /// Run the workload once.
    fn run(&self, qp: &Qp);
}
