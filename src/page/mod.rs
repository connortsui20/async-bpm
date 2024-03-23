pub(crate) mod eviction;
pub(crate) mod page_guard;
pub(crate) mod page_handle;

mod page_inner;

pub(crate) use crate::page::page_inner::Page;
