//! Cache of created `ibv_ah` objects.
#![allow(unused)]

use std::cell::RefCell;

use lazy_static::lazy_static;
use mini_moka::unsync::Cache as LocalCache;
use moka::sync::Cache as GlobalCache;
use rrddmma::prelude::*;

const LOCAL_CAPACITY: u64 = 64;
const GLOBAL_CAPACITY: u64 = 4096;

/// Node ID and DCT number pair.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[repr(C)]
pub(crate) struct NodeDct {
    /// Node ID.
    pub node: u16,

    /// DCT number.
    pub dct_num: u32,
}

impl NodeDct {
    /// Create a new `NodeDct` instance.
    pub fn new(node: u16, dct_num: u32) -> Self {
        Self { node, dct_num }
    }
}

thread_local! {
    /// Thread-local cache.
    static LOCAL_CACHE: RefCell<LocalCache<NodeDct, QpPeer>> = RefCell::new(LocalCache::new(LOCAL_CAPACITY));
}

lazy_static! {
    /// Shared cache.
    static ref GLOBAL_CACHE: GlobalCache<NodeDct, QpPeer> = GlobalCache::new(GLOBAL_CAPACITY);
}

/// Get-or-create an address handle in the local cache.
pub fn get_in_local_cache(key: NodeDct, qp: &Qp) -> QpPeer {
    LOCAL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        match cache.get(&key) {
            Some(peer) => peer.clone(),
            None => {
                // Assume `key.node` is the LID & symmetric cluster (i.e., all clients uses the same port ID).
                let ep = QpEndpoint::new(None, key.node, qp.port().unwrap().0.num(), key.dct_num);
                let peer = qp.make_peer(ep).unwrap();
                cache.insert(key, peer.clone());
                peer
            }
        }
    })
}

/// Get-or-create an address handle in the global cache.
pub fn get_in_global_cache(key: NodeDct, qp: &Qp) -> QpPeer {
    GLOBAL_CACHE.get_with(key, || {
        // Assume `key.node` is the LID & symmetric cluster (i.e., all clients uses the same port ID).
        let ep = QpEndpoint::new(None, key.node, qp.port().unwrap().0.num(), key.dct_num);
        qp.make_peer(ep).unwrap()
    })
}
