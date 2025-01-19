//! Trace loader and player.

use std::io::{self, BufRead};
use std::path::Path;

/// Maximum number of lock acquisitions per transaction.
pub const MAX_LOCKS_PER_TXN: usize = 16;

/// Lock acquisition item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LockInstr {
    /// Lock ID.
    pub id: u32,

    /// Lock type.
    pub shared: bool,
}

impl LockInstr {
    pub fn new(id: u32, shared: bool) -> Self {
        Self { id, shared }
    }
}

impl PartialOrd for LockInstr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LockInstr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

/// Transaction item.
#[derive(Debug, Clone, Copy)]
pub struct TxnItem {
    /// Transaction ID.
    pub id: u32,

    /// Transaction type.
    pub ty: u8,

    /// Number of locks to acquire.
    pub len: usize,

    /// Locks to acquire.
    pub locks: [LockInstr; MAX_LOCKS_PER_TXN],
}

impl TxnItem {
    /// Return a slice of the lock acquisitions contained in the transaction.
    pub fn locks(&self) -> &[LockInstr] {
        &self.locks[..self.len]
    }

    /// Return a mutable slice of the lock acquisitions contained in the transaction.
    pub fn locks_mut(&mut self) -> &mut [LockInstr] {
        &mut self.locks[..self.len]
    }
}

impl Default for TxnItem {
    fn default() -> Self {
        Self {
            id: 0,
            ty: 0,
            len: 0,
            locks: [LockInstr::default(); MAX_LOCKS_PER_TXN],
        }
    }
}

/// Loaded trace.
#[derive(Default)]
pub struct Trace {
    /// Trace path.
    path: String,

    /// Transactions contained in the trace.
    txns: Vec<TxnItem>,
}

impl std::fmt::Debug for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Trace").field("path", &self.path).finish()
    }
}

impl Trace {
    /// Load a trace from a trace file generated by FissLock.
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        // format: <TxnID>,0,<TxnType>,<LockID>,<LockType>
        // -----------------------------------------------
        // <TxnID>: u64
        // <TxnType>: ANY NUMBER
        // <LockType>:
        //   - 1 = SHARED
        //   - 2 = EXCLUSIVE

        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = std::fs::File::open(path)?;
        let mut txns = Vec::new();

        let mut cur_txn = TxnItem::default();

        for line in io::BufReader::new(file).lines() {
            let line = line?;
            let mut iter = line.split(',');

            let txn_id = iter.next().unwrap().parse().unwrap();
            let _ = iter.next().unwrap();
            let ty = iter.next().unwrap().parse::<u8>().unwrap() - 1;
            if txn_id != cur_txn.id {
                if cur_txn.id != 0 {
                    cur_txn.locks[..cur_txn.len].sort();
                    txns.push(cur_txn);
                }
                cur_txn = TxnItem {
                    id: txn_id,
                    ty,
                    len: 0,
                    locks: [LockInstr::default(); MAX_LOCKS_PER_TXN],
                };
            }

            let lock_id = iter.next().unwrap().parse().unwrap();
            let shared = iter.next().unwrap().parse::<u8>().unwrap() == 1;
            assert!(iter.next().is_none());
            cur_txn.locks[cur_txn.len] = LockInstr::new(lock_id, shared);
            cur_txn.len += 1;
        }

        Ok(Self {
            path: path_str,
            txns,
        })
    }

    /// Return the path of the trace.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Return a slice of the transactions contained in the trace.
    pub fn txns(&self) -> &[TxnItem] {
        &self.txns
    }

    /// Return a mutable slice of the transactions contained in the trace.
    pub fn txns_mut(&mut self) -> &mut [TxnItem] {
        &mut self.txns
    }
}
