//! Breakdown timing.

use std::cell::Cell;
use std::io;
use std::sync::{Arc, Mutex};

use super::TimerOp;
use crate::utils::Timer;

use lazy_static::lazy_static;
use strum::EnumCount;

const TIMER_COUNT: usize = TimeItem::COUNT as usize;

#[derive(strum::EnumCount, strum::EnumIter, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeItem {
    AcqInitialCasOrWriterFaa = 0,
    AcqInitialReaderFaa,
    AcqNotify,
    AcqWaitPred,
    AcqWaitReadersOrBackoff,
    AcqWaitWritersOrRetry,
    AcqCheckFailure,

    RelInitialAtomic,
    RelTransferAtomic,
    RelWaitSucc,
    RelNotify,
}

#[allow(unused)]
struct TypedOp {
    ty: TimeItem,
    op: TimerOp<'static>,
    rebegin: bool,
}

lazy_static! {
    pub static ref RESULTS: [Arc<Mutex<Timer>>; TIMER_COUNT] =
        std::array::from_fn(|_| Arc::new(Mutex::new(Timer::new())));
}

thread_local! {
    static TIMERS: [Timer; TIMER_COUNT] = [
        Timer::new(),
        Timer::new(),
        Timer::new(),
        Timer::new(),
        Timer::new(),
        Timer::new(),
        Timer::new(),

        Timer::new(),
        Timer::new(),
        Timer::new(),
        Timer::new(),
    ];
    static CURRENT_OPS: [Cell<Option<TypedOp>>; TIMER_COUNT] = [
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),

        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
        Cell::new(None),
    ];
}

#[cfg(not(feature = "timed"))]
mod timing_impl {
    use super::*;

    pub fn begin_time_op(_: TimeItem) {}
    pub fn rebegin_time_op(_: TimeItem) {}
    pub fn end_time_op(_: TimeItem) {}
    pub fn zero_time_op(_: TimeItem) {}
    pub fn commit() {}
    pub fn report(_: &mut dyn io::Write) {}
}

#[cfg(feature = "timed")]
mod timing_impl {
    use super::*;
    use std::time::Duration;
    use strum::IntoEnumIterator;

    fn begin_time_op_re(ty: TimeItem, rebegin: bool) {
        let timer = unsafe {
            let p = TIMERS.with(|t| {
                let timers = t.as_ptr();
                timers.add(ty as u8 as usize)
            });
            (p as *mut Timer).as_mut().unwrap()
        };
        let op = TimerOp::new(timer);
        CURRENT_OPS.with(|op_cells| {
            op_cells[ty as usize].set(Some(TypedOp { ty, op, rebegin }));
        });
    }

    pub fn begin_time_op(ty: TimeItem) {
        begin_time_op_re(ty, false);
    }

    pub fn rebegin_time_op(ty: TimeItem) {
        begin_time_op_re(ty, true);
    }

    pub fn end_time_op(ty: TimeItem) {
        CURRENT_OPS.with(|op_cells| {
            let op = op_cells[ty as usize].replace(None);
            if let Some(op) = op {
                assert_eq!(op.ty, ty);
                let rebegin = op.rebegin;
                drop(op);

                if rebegin {
                    let timer = unsafe {
                        let p = TIMERS.with(|t| {
                            let timers = t.as_ptr();
                            timers.add(ty as u8 as usize)
                        });
                        (p as *mut Timer).as_mut().unwrap()
                    };
                    timer.digest_last();
                }
            }
        });
    }

    pub fn zero_time_op(ty: TimeItem) {
        let timer = unsafe {
            let p = TIMERS.with(|t| {
                let timers = t.as_ptr();
                timers.add(ty as u8 as usize)
            });
            (p as *mut Timer).as_mut().unwrap()
        };
        timer.push(Duration::ZERO);
    }

    pub fn commit() {
        for (i, t) in RESULTS.iter().enumerate() {
            let mut timer = t.lock().unwrap();
            let my_timer = unsafe {
                let p = TIMERS.with(|t| {
                    let timers = t.as_ptr();
                    timers.add(i)
                });
                (p as *mut Timer).as_mut().unwrap()
            };
            timer.merge_mut(my_timer);
        }
    }

    pub fn report(w: &mut dyn io::Write) {
        for ty in TimeItem::iter() {
            let timer = &RESULTS[ty as u8 as usize];
            let mut timer = timer.lock().unwrap();
            let stats = timer.report();

            writeln!(w, "Timer {} {:?}:", ty as u8, ty).unwrap();
            writeln!(w, "  Average: {:?}", stats.avg).unwrap();
            writeln!(w, "  50th percentile: {:?}", stats.cdf[50]).unwrap();
            writeln!(w, "  90th percentile: {:?}", stats.cdf[90]).unwrap();
            writeln!(w, "  99th percentile: {:?}", stats.cdf[99]).unwrap();
            writeln!(w, "").unwrap();
        }
    }
}

pub use timing_impl::*;
