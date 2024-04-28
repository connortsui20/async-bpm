//! Definitions and types related to logical pages of data.

use crate::{bpm::BufferPoolManager, disk::frame::Frame};
use derivative::Derivative;
use std::{fmt::Display, sync::Arc};
use tokio::sync::RwLock;

/// The size of a buffer [`Frame`] / logical [`Page`] of data.
pub const PAGE_SIZE: usize = 1 << 12;

/// A shared logical [`Page`] object. All access should be done through a
/// [`PageHandle`](super::PageHandle).
#[derive(Derivative)]
#[derivative(Debug, PartialEq, Eq, Hash)]
pub struct Page {
    /// The unique ID of this logical page of data.
    pub(crate) pid: PageId,

    /// An optional pointer to a buffer [`Frame`], protected by a [`RwLock`].
    ///
    /// Either a page's data is in a [`Frame`] in memory, or it is only stored on disk / permanent
    /// storage. In either case, it is protected by a read-write lock to ensure that multiple
    /// threads and tasks can access the optional frame with proper synchronization.
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) inner: RwLock<Option<Frame>>,

    /// A pointer back to to the buffer pool manager.
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) bpm: Arc<BufferPoolManager>,
}

/// A reference-counted reference to a [`Page`].
pub type PageRef = Arc<Page>;

/// A unique identifier for a shared [`Page`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    /// Inner representation subject to change...
    inner: u64,
}

impl Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page {}", self.inner)
    }
}

/// Implementation of this type subject to change...
#[allow(missing_docs)]
impl PageId {
    pub fn new(id: u64) -> Self {
        Self { inner: id }
    }

    pub fn as_u64(self) -> u64 {
        self.inner
    }

    pub fn fd(&self) -> u32 {
        todo!("Which file descriptor (which physical disk) does this belong to?")
    }

    pub fn offset(&self) -> u64 {
        self.as_u64() * PAGE_SIZE as u64
    }
}

/// A `PageId` must always be convertible into a unique 64-bit integer.
impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.as_u64()
    }
}
