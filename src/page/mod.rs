mod page_guard;
mod page_handle;

pub(crate) mod eviction;

use crate::{bpm::BufferPoolManager, disk::frame::Frame};
use derivative::Derivative;
use eviction::Temperature;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use page_guard::*;
pub use page_handle::*;

/// The size of a buffer [`Frame`] / logical [`Page`] of data.
pub const PAGE_SIZE: usize = 1 << 12;

/// A shared logical [`Page`] object. All access should be done through a [`PageHandle`]()
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Page {
    pub(crate) pid: PageId,
    pub(crate) eviction_state: Temperature,
    pub(crate) inner: RwLock<Option<Frame>>, // TODO change to hybrid latch

    #[derivative(Debug = "ignore")]
    pub(crate) bpm: Arc<BufferPoolManager>,
}

pub type PageRef = Arc<Page>;

/// A unique identifier for a shared [`Page`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    inner: u64,
}

impl PageId {
    pub fn new(id: u64) -> Self {
        Self { inner: id }
    }
}

/// We must always be able to convert a `PageId` into a unique 64-bit integer.
impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.inner
    }
}
