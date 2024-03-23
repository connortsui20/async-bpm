use crate::page::eviction::HOT;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::Page;
use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};

pub struct PageHandle {
    page: Arc<Page>,
}

impl PageHandle {
    pub async fn read(&self) -> ReadPageGuard {
        self.page.eviction_state.store(HOT, Ordering::Release);

        {
            let guard = self.page.inner.read().await;

            // If it is already loaded, then we're done
            if guard.deref().is_some() {
                return ReadPageGuard::new(guard);
            }
        }

        // We need to load the page into memory
        let guard = self.page.inner.write().await;

        todo!()
    }

    pub async fn write(&self) -> WritePageGuard {
        todo!()
    }
}
