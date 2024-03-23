pub(crate) mod eviction;
pub(crate) mod page_guard;
pub(crate) mod page_handle;

mod page_inner;

pub(crate) use crate::page::page_inner::Page;

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
