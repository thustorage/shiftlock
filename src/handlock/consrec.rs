//! Module for recording handover events.

use std::cell::RefCell;
use std::mem;
use std::sync::{atomic::*, Arc, Mutex};

use lazy_static::lazy_static;

static HANDOVER_CNT: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    static ref GLOBAL_CONS_WRITER: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
}

thread_local! {
    static CONSWRT: RefCell<Vec<u32>> = RefCell::new(Vec::new());
    static CONSREC_ENABLED: AtomicBool = AtomicBool::new(false);
}

pub fn set_wrt_rec(enabled: bool) {
    CONSREC_ENABLED.with(|enabled_cell| {
        enabled_cell.store(enabled, Ordering::Relaxed);
    });
}

pub fn cons_wrt_rec(val: u32) {
    let enabled = CONSREC_ENABLED.with(|enabled_cell| enabled_cell.load(Ordering::Relaxed));
    if enabled {
        CONSWRT.with(|consrec_cell| {
            consrec_cell.borrow_mut().push(val);
        });
    }
}

pub fn handover_rec() {
    HANDOVER_CNT.fetch_add(1, Ordering::Relaxed);
}

/// Commit thread-local consrec data to global.
pub fn commit() {
    let mut consrec = Vec::new();
    CONSWRT.with(|consrec_cell| {
        let mut cr = consrec_cell.borrow_mut();
        mem::swap(&mut consrec, &mut *cr);
    });
    GLOBAL_CONS_WRITER.lock().unwrap().extend(consrec);
}

/// Take all collected writer consrec data.
#[must_use = "this function returns the collected consrec data"]
pub fn get() -> Vec<usize> {
    let mut consrec = Vec::new();
    let mut global = GLOBAL_CONS_WRITER.lock().unwrap();
    mem::swap(&mut consrec, &mut *global);
    drop(global);

    if consrec.is_empty() {
        return Vec::new();
    }
    consrec.sort_unstable();

    let n = *consrec.last().unwrap();
    let mut ret = vec![0; n as usize + 1];
    for &val in &consrec {
        ret[val as usize] += 1;
    }
    ret
}

/// Get the number of handovers.
#[must_use = "this function returns the number of handovers"]
pub fn get_handover_cnt() -> usize {
    HANDOVER_CNT.load(Ordering::Relaxed)
}
