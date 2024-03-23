use super::{
    eviction::{Temperature, HOT},
    page_guard::{ReadPageGuard, WritePageGuard},
};
use crate::frame::Frame;
use std::{ops::Deref, sync::atomic::Ordering};
use tokio::sync::RwLock;

type PageInner = Option<Frame>;

pub(crate) struct Page {
    eviction_state: Temperature,
    inner: RwLock<PageInner>, // TODO change to hybrid latch
}

impl Page {
    pub(crate) async fn read(&self) -> ReadPageGuard {
        self.eviction_state.store(HOT, Ordering::Release);

        {
            let guard = self.inner.read().await;

            // If it is already loaded, then we're done
            if guard.deref().is_some() {
                return ReadPageGuard::new(guard);
            }
        }

        // We need to load the page into memory
        let guard = self.inner.write().await;

        todo!()
    }

    pub(crate) async fn write(&self) -> WritePageGuard {
        todo!()
    }
}
