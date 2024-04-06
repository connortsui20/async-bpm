//! Implementation of the `PageHandle` type.

use super::eviction::TemperatureState;
use super::PageRef;
use crate::bpm::BufferPoolManager;
use crate::disk::disk_manager::DiskManagerHandle;
use crate::disk::frame::Frame;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use std::sync::Arc;
use std::{ops::Deref, sync::atomic::Ordering};
use tokio::sync::RwLockWriteGuard;

/// A thread-local handle to a logical page of data.
#[derive(Debug)]
pub struct PageHandle {
    pub(crate) page: PageRef,
    pub(crate) bpm: Arc<BufferPoolManager>,
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
            .bpm
            .free_frames
            .1
            .recv()
            .await
            .expect("Free frames channel was unexpectedly closed");

        assert!(frame.owner.is_none());

        // Read the data in from disk via the disk manager
        let mut frame = self
            .dm
            .read_into(self.page.pid, frame)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Was unable to read data from page {:?} from disk",
                    self.page.pid
                )
            });

        // Make the current page the frame's owner
        frame.owner.replace(self.page.clone());

        // Add to the set of active pages
        let mut active_guard = self.bpm.active_pages.lock().await;
        active_guard.insert(self.page.pid);

        // Update the eviction state
        self.page
            .eviction_state
            .store(TemperatureState::Hot, Ordering::Release);

        todo!()
    }

    pub async fn evict(&self) {
        let mut guard = self.page.inner.write().await;

        if guard.deref().is_none() {
            // There is nothing for us to evict
            return;
        }

        // Remove from the set of active pages
        let mut active_guard = self.bpm.active_pages.lock().await;
        let remove_res = active_guard.remove(&self.page.pid);
        assert!(
            remove_res,
            "Removed an active page that was somehow not in the active pages set"
        );

        let frame = guard.take().unwrap();

        // Write the data out to disk
        let frame = self
            .dm
            .write_from(self.page.pid, frame)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Was unable to write data from page {:?} to disk",
                    self.page.pid
                )
            });

        // Update the eviction state
        self.page
            .eviction_state
            .store(TemperatureState::Cold, Ordering::Release);

        // Free the frame
        self.bpm
            .free_frames
            .0
            .send(frame)
            .await
            .expect("Free frames channel was unexpectedly closed");
    }
}
