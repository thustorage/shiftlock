//! Trivial RPC lock implementation.

use rrddmma::prelude::*;
use std::{mem, ptr};

pub struct RpcLock<'a> {
    local: MrSlice<'a>,
    lock_id: u64,
}

use crate::{LOCK_ACQUIRE_EX, LOCK_ACQUIRE_SH, LOCK_RELEASE};

impl<'a> RpcLock<'a> {
    pub fn new(local: MrSlice<'a>, lock_id: u64) -> Self {
        debug_assert!(local.len() >= 3 * mem::size_of::<u64>());
        RpcLock { local, lock_id }
    }

    pub async fn acquire(self, qp: &Qp, shared: bool) -> RpcLockGuard<'a> {
        let send_buf = self.local.slice(0, 2 * mem::size_of::<u64>()).unwrap();
        let recv_buf = self
            .local
            .slice(2 * mem::size_of::<u64>(), mem::size_of::<u64>())
            .unwrap();

        loop {
            qp.recv(&[recv_buf], 0).unwrap();

            unsafe {
                ptr::write::<u64>(
                    send_buf.addr() as *mut u64,
                    if shared {
                        LOCK_ACQUIRE_SH
                    } else {
                        LOCK_ACQUIRE_EX
                    },
                );
                ptr::write::<u64>(
                    send_buf.addr().add(mem::size_of::<u64>()) as *mut u64,
                    self.lock_id,
                );
            }

            qp.send(&[send_buf], None, None, 0, true, true).unwrap();
            qp.scq().poll_one_blocking_consumed();
            qp.rcq().poll_one_blocking_consumed();

            let lock = unsafe { ptr::read::<u64>(recv_buf.addr() as *const u64) };
            if lock == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        RpcLockGuard { lock: self }
    }
}

pub struct RpcLockGuard<'a> {
    lock: RpcLock<'a>,
}

impl<'a> RpcLockGuard<'a> {
    pub fn release(self, qp: &Qp) -> RpcLock<'a> {
        let send_buf = self.lock.local.slice(0, 2 * mem::size_of::<u64>()).unwrap();
        let recv_buf = self
            .lock
            .local
            .slice(2 * mem::size_of::<u64>(), mem::size_of::<u64>())
            .unwrap();

        qp.recv(&[recv_buf], 0).unwrap();

        unsafe {
            ptr::write::<u64>(send_buf.addr() as *mut u64, LOCK_RELEASE);
            ptr::write::<u64>(
                send_buf.addr().add(mem::size_of::<u64>()) as *mut u64,
                self.lock.lock_id,
            );
        }
        qp.send(&[send_buf], None, None, 0, true, true).unwrap();
        qp.scq().poll_one_blocking_consumed();
        qp.rcq().poll_one_blocking_consumed();

        self.lock
    }
}
