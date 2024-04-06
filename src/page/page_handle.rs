//! Implementation of the `PageHandle` type.

use super::eviction::TemperatureState;
use super::PageRef;
use crate::disk::disk_manager::DiskManagerHandle;
use crate::disk::frame::Frame;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use std::{ops::Deref, sync::atomic::Ordering};
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
#[derive(Debug)]
pub struct PageHandle {
    pub(crate) page: PageRef,
    pub(crate) dm: DiskManagerHandle,
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
        // If someone else got in front of us and loaded the page for us
        if guard.deref().is_some() {
            return;
        }

        // Wait for a free frame
        let frame = self
            .page
            .bpm
            .free_frames
            .1
            .recv()
            .await
            .expect("channel was unexpectedly closed");

        assert!(frame.owner.is_none());

        let mut frame = self.dm.read(self.page.pid, frame).await.unwrap();

        // Make the current page the frame's owner
        frame.owner.replace(self.page.clone());

        self.page
            .eviction_state
            .store(TemperatureState::Hot, Ordering::Release);

        todo!()
    }
}
