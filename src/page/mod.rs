pub(crate) mod eviction;
pub(crate) mod page_guard;
pub(crate) mod page_handle;
pub(crate) mod page_inner;

use eviction::Temperature;
use page_inner::PageInner;
use tokio::sync::RwLock;

pub(crate) struct Page {
    eviction_state: Temperature,
    inner: RwLock<PageInner>, // TODO change to hybrid latch
}
