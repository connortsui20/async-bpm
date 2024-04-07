mod page_guard;
mod page_handle;

pub(crate) mod eviction;

use crate::disk::frame::Frame;
use eviction::Temperature;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use page_guard::*;
pub use page_handle::*;

/// The size of a buffer [`Frame`] / logical [`Page`] of data.
pub const PAGE_SIZE: usize = 1 << 12;

/// A shared logical [`Page`] object. All access should be done through a [`PageHandle`]()
#[derive(Debug)]
pub struct Page {
    pub(crate) pid: PageId,
    pub(crate) eviction_state: Temperature,
    pub(crate) inner: RwLock<Option<Frame>>, // TODO change to hybrid latch
}

/// A shared reference to a [`Page`].
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

    pub fn as_u64(self) -> u64 {
        Into::<u64>::into(self)
    }

    pub fn fd(&self) -> u32 {
        todo!()
    }

    pub fn offset(&self) -> u64 {
        self.inner * PAGE_SIZE as u64
    }

    pub fn buf_group(&self) -> u16 {
        todo!()
    }

    pub fn buf_index(&self) -> u16 {
        todo!()
    }
}

/// We must always be able to convert a `PageId` into a unique 64-bit integer.
impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.inner
    }
}
