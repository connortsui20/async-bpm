//! Implementation of the `PageHandle` type.

use super::eviction::TemperatureState;
use super::PageRef;
use crate::disk::frame::Frame;
use crate::io::IoUringAsync;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use std::{ops::Deref, sync::atomic::Ordering};
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
pub struct PageHandle {
    pub(crate) page: PageRef,
    pub(crate) uring: IoUringAsync,
}

impl PageHandle {
    /// Gets a read guard on a logical page, which guarantees the data is in memory.
    pub async fn read(&self) -> ReadPageGuard {
        self.page
            .eviction_state
            .store(TemperatureState::Hot, Ordering::Release);

        let read_guard = self.page.inner.read().await;

        // If it is already loaded, then we're done
        if read_guard.deref().is_some() {
            return ReadPageGuard::new(read_guard);
        }

        // Otherwise we need to load the page into memory with a write guard
        drop(read_guard);
        let mut write_guard = self.page.inner.write().await;

        self.load(&mut write_guard).await;

        ReadPageGuard::new(write_guard.downgrade())
    }

    /// Gets a write guard on a logical page, which guarantees the data is in memory.
    pub async fn write(&self) -> WritePageGuard {
        self.page
            .eviction_state
            .store(TemperatureState::Hot, Ordering::Release);

        let mut write_guard = self.page.inner.write().await;

        // If it is already loaded, then we're done
        if write_guard.deref().is_some() {
            return WritePageGuard::new(write_guard);
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        WritePageGuard::new(write_guard)
    }

    /// Loads page data from disk into a frame in memory.
    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        if guard.deref().is_some() {
            // Someone else got in front of us and loaded the page for us
            return;
        }

        // Wait for a free frame
        let mut frame = self
            .page
            .bpm
            .free_frames_rx
            .recv()
            .await
            .expect("channel was unexpectedly closed");

        // Add the current page
        assert!(frame.parent.is_none());
        frame.parent.replace(self.page.clone());

        todo!("Read in the page's data from disk");

        self.page
            .eviction_state
            .store(TemperatureState::Hot, Ordering::Release);
    }
}
