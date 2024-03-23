use super::{
    eviction::{Temperature, HOT},
    page_guard::{ReadPageGuard, WritePageGuard},
    PageId,
};
use crate::{bpm::BufferPoolManager, frame::Frame};
use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::{RwLock, RwLockWriteGuard};

type PageInner = Option<Frame>;

pub(crate) struct Page {
    pid: PageId,
    eviction_state: Temperature,
    inner: RwLock<PageInner>, // TODO change to hybrid latch
    bpm: Arc<BufferPoolManager>,
}

impl Page {
    pub fn id(&self) -> PageId {
        self.pid
    }

    pub(crate) async fn read(&self) -> ReadPageGuard {
        self.eviction_state.store(HOT, Ordering::Release);

        let read_guard = self.inner.read().await;

        // If it is already loaded, then we're done
        if read_guard.deref().is_some() {
            return ReadPageGuard::new(read_guard);
        }

        drop(read_guard);

        // We need to load the page into memory
        let mut write_guard = self.inner.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(write_guard.downgrade())
    }

    pub(crate) async fn write(&self) -> WritePageGuard {
        todo!()
    }

    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        if guard.deref().is_some() {
            // Someone else got in front of us and loaded the page for us
            return;
        }

        if let Some(mut frame) = self.bpm.free_frames.pop() {
            let pages_guard = self.bpm.pages.read().await;
            let current_page_ptr = pages_guard
                .get(&self.pid)
                .expect("Couldn't find ourselves in the global table of pages");

            assert!(frame.parent.is_none());
            frame.parent.replace(current_page_ptr.clone());

            todo!("Read our page's data from disk via a disk manager and await")
        }

        todo!("Else we need to evict")
    }
}
