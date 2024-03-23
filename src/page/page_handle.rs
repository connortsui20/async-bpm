use super::eviction::HOT;
use crate::frame::Frame;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::Page;
use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::RwLockWriteGuard;

pub struct PageHandle {
    pub(crate) page: Arc<Page>,
    // uring: Rc<IoUringAsync>,
}

impl PageHandle {
    pub(crate) async fn read(&self) -> ReadPageGuard {
        self.page.eviction_state.store(HOT, Ordering::Release);

        let read_guard = self.page.inner.read().await;

        // If it is already loaded, then we're done
        if read_guard.deref().is_some() {
            return ReadPageGuard::new(read_guard);
        }

        // We need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.inner.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(write_guard.downgrade())
    }

    pub(crate) async fn write(&self) -> WritePageGuard {
        self.page.eviction_state.store(HOT, Ordering::Release);

        let mut write_guard = self.page.inner.write().await;

        // If it is already loaded, then we're done
        if write_guard.deref().is_some() {
            return WritePageGuard::new(write_guard);
        }

        self.load(&mut write_guard).await;

        WritePageGuard::new(write_guard)
    }

    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        if guard.deref().is_some() {
            // Someone else got in front of us and loaded the page for us
            return;
        }

        if let Some(mut frame) = self.page.bpm.free_frames.pop() {
            let pages_guard = self.page.bpm.pages.read().await;
            let current_page_ptr = pages_guard
                .get(&self.page.pid)
                .expect("Couldn't find ourselves in the global table of pages");

            assert!(frame.parent.is_none());
            frame.parent.replace(current_page_ptr.clone());

            todo!("Read our page's data from disk via a disk manager and await")
        }

        todo!("Else we need to evict")
    }
}
