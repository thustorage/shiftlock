//! Zeroes the server memory.

use std::{net::SocketAddr, ptr};

use clap::Parser;
use shiftlock::utils::*;
use rrddmma::{prelude::*, wrap::RegisteredMem};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Server address.
    #[clap(short, long, default_value = "127.0.0.1:31850")]
    pub server: SocketAddr,

    /// RDMA device to use.
    #[clap(short, long, default_value = "mlx5_0")]
    pub dev: String,
}

fn main() {
    let args = Args::parse();
    let (qp, remote) = make_connected_qp(&args.dev, args.server);

    let mem = RegisteredMem::new(qp.pd(), remote.len()).expect("cannot register memory");
    unsafe { ptr::write_bytes(mem.addr(), 0, remote.len()) };
    qp.write(&[mem.as_slice()], &remote, 0, None, true)
        .expect("failed to post initialization write");
    qp.scq().poll_one_blocking_consumed();

    println!("ok");
}
