//! Implementation of the `PageHandle` type.

use crate::bpm::BufferPoolManager;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::Page;
use crate::storage::frame::Frame;
use crate::storage::storage_manager::StorageManagerHandle;
use derivative::Derivative;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct PageHandle {
    /// A shared pointer to the [`Page`](super::Page) object.
    pub(crate) page: Arc<Page>,

    /// A thread-local handle to the storage manager.
    ///
    /// By including this field, [`PageHandle`] is `!Send` and `!Sync`.
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) dm: StorageManagerHandle,
}

impl PageHandle {
    /// Creates a new page handle.
    pub(crate) fn new(page: Arc<Page>, dm: StorageManagerHandle) -> Self {
        Self { page, dm }
    }

    /// Gets a read guard on a logical page, which guarantees the data is in memory.
    pub async fn read(&self) -> ReadPageGuard {
        let read_guard = self.page.frame.read().await;

        // If it is already loaded, then we're done
        if let Some(frame) = read_guard.deref() {
            frame.record_access().await;
            return ReadPageGuard::new(self.page.pid, read_guard);
        }

        // Otherwise we need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.frame.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(self.page.pid, write_guard.downgrade())
    }

    /// Attempts to grab the read lock. If unsuccessful, this function does nothing. Otherwise, this
    /// function behaves identically to [`PageHandle::read`].
    pub async fn try_read(&self) -> Option<ReadPageGuard> {
        let Ok(read_guard) = self.page.frame.try_read() else {
            return None;
        };

        // If it is already loaded, then we're done
        if let Some(frame) = read_guard.deref() {
            frame.record_access().await;
            return Some(ReadPageGuard::new(self.page.pid, read_guard));
        }

        // Otherwise we need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.frame.write().await;

        self.load(&mut write_guard).await;

        Some(ReadPageGuard::new(self.page.pid, write_guard.downgrade()))
    }

    /// Gets a write guard on a logical page, which guarantees the data is in memory.
    pub async fn write(&self) -> WritePageGuard {
        let mut write_guard = self.page.frame.write().await;

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access().await;
            return WritePageGuard::new(self.page.pid, write_guard);
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        WritePageGuard::new(self.page.pid, write_guard)
    }

    /// Attempts to grab the write lock. If unsuccessful, this function does nothing. Otherwise,
    /// this function behaves identically to [`PageHandle::write`].
    pub async fn try_write(&self) -> Option<WritePageGuard> {
        let Ok(mut write_guard) = self.page.frame.try_write() else {
            return None;
        };

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access().await;
            return Some(WritePageGuard::new(self.page.pid, write_guard));
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        Some(WritePageGuard::new(self.page.pid, write_guard))
    }

    /// Loads page data from persistent storage into a frame in memory.
    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        // If someone else got in front of us and loaded the page for us
        if let Some(frame) = guard.deref().deref() {
            frame.record_access().await;
            return;
        }

        // Randomly choose a `FrameGroup` to place load this page into
        let bpm = BufferPoolManager::get();
        let frame_group = bpm.get_random_frame_group();

        // Wait for a free frame
        let mut frame = frame_group.get_free_frame().await;
        let none = frame.replace_page_owner(self.page.clone());
        assert!(none.is_none());

        // Read the data in from persistent storage via the storage manager handle
        let (res, frame) = self.dm.read_into(self.page.pid, frame).await;
        res.expect("TODO");

        // Give ownership of the frame to the actual page
        let old: Option<Frame> = guard.replace(frame);
        assert!(old.is_none());
    }
}
