pub(crate) mod eviction;
pub(crate) mod page_guard;
pub(crate) mod page_handle;

use crate::{bpm::BufferPoolManager, frame::Frame};
use eviction::Temperature;
use std::sync::Arc;
use tokio::sync::RwLock;

pub const PAGE_SIZE: usize = 1 << 12;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    inner: u64,
}

/// We must always be able to convert a `PageId` into a unique 64-bit integer.
impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.inner
    }
}

type PageInner = Option<Frame>;

pub(crate) struct Page {
    pub(crate) pid: PageId,
    pub(crate) eviction_state: Temperature,
    pub(crate) inner: RwLock<PageInner>, // TODO change to hybrid latch
    pub(crate) bpm: Arc<BufferPoolManager>,
}
