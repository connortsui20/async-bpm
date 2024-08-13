//! Implementation of the `PageHandle` type.
//!
//! Users of this library are expected to interact with data in this buffer pool via
//! [`PageHandle`]s, and thus the only way to get to the data is to request a [`PageHandle`] through
//! the [`BufferPoolManager`] and then construct a [`ReadPageGuard`] or [`WritePageGuard`] through
//! one of the methods on [`PageHandle`].

use crate::bpm::BufferPoolManager;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::Page;
use crate::storage::frame::Frame;
use crate::storage::storage_manager::StorageManagerHandle;
use derivative::Derivative;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct PageHandle {
    /// A shared pointer to the [`Page`] object.
    pub(crate) page: Arc<Page>,

    /// A thread-local handle to the storage manager.
    ///
    /// By including this field, `PageHandle` is `!Send` and `!Sync`.
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) sm: StorageManagerHandle,
}

impl PageHandle {
    /// Creates a new page handle.
    pub(crate) fn new(page: Arc<Page>, sm: StorageManagerHandle) -> Self {
        Self { page, sm }
    }

    /// Gets a read guard on a logical page, which guarantees the data is in memory.
    ///
    /// # Panics
    ///
    /// Fatally panics if an I/O error occurs while trying to bring the data into memory.
    pub async fn read(&self) -> ReadPageGuard {
        // Optimization: attempt to read only if we observe that the `is_loaded` flag is set.
        if self.page.is_loaded.load(Ordering::Acquire) {
            let read_guard = self.page.frame.read().await;

            // If it is already loaded, then we're done.
            if let Some(frame) = read_guard.deref() {
                self.page.is_loaded.store(true, Ordering::Release);
                frame.record_access(self.page.clone());
                return ReadPageGuard::new(self.page.pid, read_guard);
            }

            // Otherwise someone evicted the page underneath us and we need to load the page into
            // memory with a write guard.
            drop(read_guard);
        }

        let mut write_guard = self.page.frame.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(self.page.pid, write_guard.downgrade())
    }

    /// Attempts to optimistically get a read guard _without_ blocking.
    ///
    /// If unsuccessful, this function does nothing and returns `None`. Otherwise, this function
    /// behaves identically to [`PageHandle::read`].
    ///
    /// # Panics
    ///
    /// Fatally panics if an I/O error occurs while trying to bring the data into memory.
    pub async fn try_read(&self) -> Option<ReadPageGuard> {
        // Optimization: attempt to read only if we observe that the `is_loaded` flag is set.
        if self.page.is_loaded.load(Ordering::Acquire) {
            let Ok(read_guard) = self.page.frame.try_read() else {
                return None;
            };

            // If it is already loaded, then we're done.
            if let Some(frame) = read_guard.deref() {
                self.page.is_loaded.store(true, Ordering::Release);
                frame.record_access(self.page.clone());
                return Some(ReadPageGuard::new(self.page.pid, read_guard));
            }

            // Otherwise someone evicted the page underneath us and we need to load the page into
            // memory with a write guard.
            drop(read_guard);
        }

        let mut write_guard = self.page.frame.write().await;

        self.load(&mut write_guard).await;

        Some(ReadPageGuard::new(self.page.pid, write_guard.downgrade()))
    }

    /// Gets a write guard on a logical page, which guarantees the data is in memory.
    ///
    /// # Panics
    ///
    /// Fatally panics if an I/O error occurs while trying to bring the data into memory.
    pub async fn write(&self) -> WritePageGuard {
        let mut write_guard = self.page.frame.write().await;

        // If it is already loaded, then we're done.
        if let Some(frame) = write_guard.deref() {
            self.page.is_loaded.store(true, Ordering::Release);
            frame.record_access(self.page.clone());
            return WritePageGuard::new(self.page.pid, write_guard);
        }

        // Otherwise we need to load the page into memory.
        self.load(&mut write_guard).await;

        WritePageGuard::new(self.page.pid, write_guard)
    }

    /// Attempts to optimistically get a write guard _without_ blocking.
    ///
    /// If unsuccessful, this function does nothing and returns `None`. Otherwise, this function
    /// behaves identically to [`PageHandle::write`].
    ///
    /// # Panics
    ///
    /// Fatally panics if an I/O error occurs while trying to bring the data into memory.
    pub async fn try_write(&self) -> Option<WritePageGuard> {
        let Ok(mut write_guard) = self.page.frame.try_write() else {
            return None;
        };

        // If it is already loaded, then we're done.
        if let Some(frame) = write_guard.deref() {
            self.page.is_loaded.store(true, Ordering::Release);
            frame.record_access(self.page.clone());
            return Some(WritePageGuard::new(self.page.pid, write_guard));
        }

        // Otherwise we need to load the page into memory.
        self.load(&mut write_guard).await;

        Some(WritePageGuard::new(self.page.pid, write_guard))
    }

    /// Loads page data from persistent storage into a frame in memory.
    ///
    /// # Panics
    ///
    /// Fatally panics if an I/O error occurs while trying to bring the data into memory.
    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        // If someone else got in front of us and loaded the page for us.
        if let Some(frame) = guard.deref().deref() {
            self.page.is_loaded.store(true, Ordering::Release);
            frame.record_access(self.page.clone());
            return;
        }

        // Randomly choose a `FrameGroup` to place load this page into.
        let bpm = BufferPoolManager::get();
        let frame_group = bpm.get_random_frame_group();

        // Wait for a free frame.
        let mut frame = frame_group.get_free_frame().await.expect(
            "FATAL: Encountered an I/O error attempting to find free space for data in memory",
        );
        let none = frame.replace_page_owner(self.page.clone());
        assert!(none.is_none());

        // Read the data in from persistent storage via the storage manager handle.
        let (res, frame) = self.sm.read_into(self.page.pid, frame).await;
        res.expect("FATAL: Encountered an I/O error trying to read data from persistent storage");

        self.page.is_loaded.store(true, Ordering::Release);
        frame.record_access(self.page.clone());

        // Give ownership of the frame to the actual page.
        let old: Option<Frame> = guard.replace(frame);
        assert!(old.is_none());
    }
}
