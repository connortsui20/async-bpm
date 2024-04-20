use super::eviction::Temperature;
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
    pub(crate) pid: PageId,

    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) eviction_state: Temperature,

    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) inner: RwLock<Option<Frame>>,

    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) bpm: Arc<BufferPoolManager>,
}

/// A shared reference to a [`Page`].
pub type PageRef = Arc<Page>;

/// A unique identifier for a shared [`Page`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    inner: u64,
}

impl Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page {}", self.inner)
    }
}

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

/// We must always be able to convert a `PageId` into a unique 64-bit integer.
impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.as_u64()
    }
}
