//! Implementation of the `PageHandle` type.

use super::PageRef;
use crate::bpm::BufferPoolManager;
use crate::storage::storage_manager::DriveManagerHandle;
use crate::storage::frame::Frame;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use derivative::Derivative;
use std::ops::Deref;
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct PageHandle {
    /// A shared pointer to the [`Page`](super::Page) object.
    pub(crate) page: PageRef,

    /// A thread-local handle to the drive manager.
    ///
    /// By including this field, [`PageHandle`] is `!Send` and `!Sync`.
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) dm: DriveManagerHandle,
}

impl PageHandle {
    /// Creates a new page handle.
    pub(crate) fn new(page: PageRef, dm: DriveManagerHandle) -> Self {
        Self { page, dm }
    }

    /// Gets a read guard on a logical page, which guarantees the data is in memory.
    pub async fn read(&self) -> ReadPageGuard {
        let read_guard = self.page.inner.read().await;

        // If it is already loaded, then we're done
        if let Some(frame) = read_guard.deref() {
            frame.record_access();
            return ReadPageGuard::new(self.page.pid, read_guard);
        }

        // Otherwise we need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.inner.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(self.page.pid, write_guard.downgrade())
    }

    /// Attempts to grab the read lock. If unsuccessful, this function does nothing. Otherwise, this
    /// function behaves identically to [`PageHandle::read`].
    pub async fn try_read(&self) -> Option<ReadPageGuard> {
        let Ok(read_guard) = self.page.inner.try_read() else {
            return None;
        };

        // If it is already loaded, then we're done
        if let Some(frame) = read_guard.deref() {
            frame.record_access();
            return Some(ReadPageGuard::new(self.page.pid, read_guard));
        }

        // Otherwise we need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.inner.write().await;

        self.load(&mut write_guard).await;

        Some(ReadPageGuard::new(self.page.pid, write_guard.downgrade()))
    }

    /// Gets a write guard on a logical page, which guarantees the data is in memory.
    pub async fn write(&self) -> WritePageGuard {
        let mut write_guard = self.page.inner.write().await;

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access();
            return WritePageGuard::new(self.page.pid, write_guard);
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        WritePageGuard::new(self.page.pid, write_guard)
    }

    /// Attempts to grab the write lock. If unsuccessful, this function does nothing. Otherwise,
    /// this function behaves identically to [`PageHandle::write`].
    pub async fn try_write(&self) -> Option<WritePageGuard> {
        let Ok(mut write_guard) = self.page.inner.try_write() else {
            return None;
        };

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access();
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
            frame.record_access();
            return;
        }

        // Randomly choose a `FrameGroup` to place load this page into
        let bpm = BufferPoolManager::get();
        let frame_group = bpm.get_random_frame_group();

        // Wait for a free frame
        let frame = frame_group.get_free_frame().await;
        frame.set_page_owner(self.page.clone());

        // Read the data in from persistent storage via the drive manager handle
        let frame = self
            .dm
            .read_into(self.page.pid, frame)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Was unable to read data from page {} from persistent storage",
                    self.page.pid
                )
            });

        // Give ownership of the frame to the actual page
        let old: Option<Frame> = guard.replace(frame);
        assert!(old.is_none());
    }
}
