//! Implementation of the `PageHandle` type.

use super::PageRef;
use crate::disk::disk_manager::DiskManagerHandle;
use crate::disk::frame::Frame;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use derivative::Derivative;
use std::ops::Deref;
use tokio::sync::RwLockWriteGuard;
use tracing::{debug, warn};

/// A thread-local handle to a logical page of data.
#[derive(Derivative)]
#[derivative(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageHandle {
    pub(crate) page: PageRef,

    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    pub(crate) dm: DiskManagerHandle,
}

impl PageHandle {
    /// Creates a new page handle.
    pub(crate) fn new(page: PageRef, dm: DiskManagerHandle) -> Self {
        Self { page, dm }
    }

    /// Gets a read guard on a logical page, which guarantees the data is in memory.
    pub async fn read(&self) -> ReadPageGuard {
        debug!("Read locking {}", self.page.pid);

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
        debug!("Try read locking {}", self.page.pid);

        let Ok(read_guard) = self.page.inner.try_read() else {
            warn!("Try read {} failed", self.page.pid);
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
        debug!("Write locking {}", self.page.pid);

        let mut write_guard = self.page.inner.write().await;

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access();
            return WritePageGuard::new(self.page.pid, write_guard, self.dm.clone());
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        WritePageGuard::new(self.page.pid, write_guard, self.dm.clone())
    }

    /// Attempts to grab the write lock. If unsuccessful, this function does nothing. Otherwise,
    /// this function behaves identically to [`PageHandle::write`].
    pub async fn try_write(&self) -> Option<WritePageGuard> {
        debug!("Try write locking {}", self.page.pid);

        let Ok(mut write_guard) = self.page.inner.try_write() else {
            warn!("Try write {} failed", self.page.pid);
            return None;
        };

        // If it is already loaded, then we're done
        if let Some(frame) = write_guard.deref() {
            frame.record_access();
            return Some(WritePageGuard::new(
                self.page.pid,
                write_guard,
                self.dm.clone(),
            ));
        }

        // Otherwise we need to load the page into memory
        self.load(&mut write_guard).await;

        Some(WritePageGuard::new(
            self.page.pid,
            write_guard,
            self.dm.clone(),
        ))
    }

    /// Loads page data from disk into a frame in memory.
    async fn load(&self, guard: &mut RwLockWriteGuard<'_, Option<Frame>>) {
        debug!("Loading {} from disk", self.page.pid);

        // If someone else got in front of us and loaded the page for us
        if let Some(frame) = guard.deref().deref() {
            frame.record_access();
            return;
        }

        todo!()

        // // Wait for a free frame
        // let frame = self
        //     .bpm
        //     .frame_groups
        //     .get_free_frame()
        //     .await
        //     .expect("Free frames channel was unexpectedly closed");

        // assert!(frame.owner.is_none());

        // // Read the data in from disk via the disk manager
        // let mut frame = self
        //     .dm
        //     .read_into(self.page.pid, frame)
        //     .await
        //     .unwrap_or_else(|_| {
        //         panic!(
        //             "Was unable to read data from page {} from disk",
        //             self.page.pid
        //         )
        //     });

        // // Make the current page the frame's owner
        // frame.owner.replace(self.page.clone());

        // // Add to the set of active pages
        // {
        //     let mut active_guard = self.bpm.active_pages.lock().await;
        //     active_guard.insert(self.page.pid);
        // }

        // // Update the eviction state
        // self.page
        //     .eviction_state
        //     .store(FrameTemperature::Hot, Ordering::Release);

        // let old = guard.replace(frame);
        // assert!(old.is_none());
    }

    // /// Cools the page, evicting it if it is already cool.
    // ///
    // /// This function will "cool" any [`Hot`](FrameTemperature::Hot) [`Page`](super::Page) down to a
    // /// [`Cool`](FrameTemperature::Cool) [`FrameTemperature`], and it will evict any
    // /// [`Cool`](FrameTemperature::Cool) [`Page`](super::Page) completely out of memory, which will
    // /// set the [`FrameTemperature`] down to [`Cold`](FrameTemperature::Cold).
    // pub(crate) async fn cool(&self) {
    //     debug!("Cooling {}", self.page.pid);

    //     match self.page.eviction_state.load(Ordering::Acquire) {
    //         FrameTemperature::Cold => panic!("Found a Cold page in the active list of pages"),
    //         FrameTemperature::Cool => self.try_evict().await,
    //         FrameTemperature::Hot => self
    //             .page
    //             .eviction_state
    //             .store(FrameTemperature::Cool, Ordering::Release),
    //     }
    // }

    // /// Evicts the page's data, freeing the [`Frame`] that this [`Page`](super::Page) owns, and
    // /// making the [`Frame`] available for other [`Page`](super::Page)s to use.
    // pub async fn evict(&self) {
    //     debug!("Evicting {}", self.page.pid);

    //     let guard = self.write().await;

    //     self.evict_inner(guard).await;
    // }

    // /// Attempts to grab the write lock. If unsuccessful, this function does nothing. Otherwise,
    // /// it behaves identically to [`PageHandle::evict`].
    // pub async fn try_evict(&self) {
    //     debug!("Try evicting {}", self.page.pid);

    //     if let Some(guard) = self.try_write().await {
    //         self.evict_inner(guard).await;
    //     } else {
    //         warn!("Eviction of {} failed", self.page.pid);
    //     }
    // }

    // /// The inner logic for evicting a page's data. See [`PageHandle::evict`] for more information.
    // async fn evict_inner(&self, mut guard: WritePageGuard<'_>) {
    //     guard.flush().await;

    //     // Remove from the set of active pages
    //     {
    //         let mut active_guard = self.bpm.active_pages.lock().await;
    //         let remove_res = active_guard.remove(&self.page.pid);
    //         assert!(
    //             remove_res,
    //             "Removed an active page that was somehow not in the active pages set"
    //         );
    //     }

    //     let mut frame = guard
    //         .guard
    //         .take()
    //         .expect("WritePageGuard somehow did not have a Frame");
    //     frame.owner = None;

    //     // Update the eviction state
    //     self.page
    //         .eviction_state
    //         .store(FrameTemperature::Cold, Ordering::Release);

    //     drop(guard);

    //     // Make the Frame available to other pages
    //     self.bpm
    //         .free_frames
    //         .0
    //         .send(frame)
    //         .await
    //         .expect("Free frames channel was unexpectedly closed");
    // }
}
