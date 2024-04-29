//! Wrappers around `tokio`'s `RwLockReadGuard` and `RwLockWriteGuard`, dedicated for pages of data.

use super::PageId;
use crate::disk::{disk_manager::DiskManagerHandle, frame::Frame};
use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use tracing::debug;

/// A read guard for a [`Page`](super::Page)'s `Frame`, which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can only be dereferenced in read mode, but other tasks (potentially on different
/// worker threads) are allowed to read from this same page.
pub struct ReadPageGuard<'a> {
    pid: PageId,
    guard: RwLockReadGuard<'a, Option<Frame>>,
}

impl<'a> ReadPageGuard<'a> {
    /// Creates a new `ReadPageGuard`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `RwLockReadGuard` holds a `None` instead of a `Some(frame)`,
    /// since we cannot have a page guard that points to nothing.
    pub(crate) fn new(pid: PageId, guard: RwLockReadGuard<'a, Option<Frame>>) -> Self {
        assert!(
            guard.deref().is_some(),
            "Cannot create a ReadPageGuard that does not own a Frame"
        );

        Self { pid, guard }
    }
}

impl<'a> Deref for ReadPageGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.guard
            .deref()
            .as_ref()
            .expect("Somehow have a ReadPageGuard without an owned frame")
    }
}

impl<'a> Drop for ReadPageGuard<'a> {
    fn drop(&mut self) {
        debug!("Dropping ReadPageGuard {}", self.pid);
    }
}

/// A write guard for a [`Page`](super::Page)'s `Frame`, which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can be dereferenced in both read and write mode, and no other tasks or threads can
/// access the page's data while a task has this guard.
pub struct WritePageGuard<'a> {
    pid: PageId,
    guard: RwLockWriteGuard<'a, Option<Frame>>,
    dmh: DiskManagerHandle,
}

impl<'a> WritePageGuard<'a> {
    /// Creates a new `WritePageGuard`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `RwLockWriteGuard` holds a `None` instead of a
    /// `Some(frame)`, since we cannot have a page guard that points to nothing.
    pub(crate) fn new(
        pid: PageId,
        guard: RwLockWriteGuard<'a, Option<Frame>>,
        dmh: DiskManagerHandle,
    ) -> Self {
        assert!(
            guard.is_some(),
            "Cannot create a WritePageGuard that does not own a Frame"
        );

        Self { pid, guard, dmh }
    }

    /// Flushes a page's data out to disk.
    pub async fn flush(&mut self) {
        debug!("Flushing {}", self.pid);

        assert!(self.guard.is_some());

        // Temporarily take ownership of the frame from the guard
        let frame = self.guard.take().unwrap();

        debug!("About to write out {} to disk", self.pid);

        // Write the data out to disk
        let frame = self
            .dmh
            .write_from(self.pid, frame)
            .await
            .unwrap_or_else(|_| panic!("Was unable to write data from page {} to disk", self.pid));

        debug!("Wrote {} to disk", self.pid);

        // Give ownership back to the guard
        self.guard.replace(frame);
    }

    /// Evicts the page from memory, consuming the guard in the process.
    pub(crate) async fn evict(mut self) -> Frame {
        assert!(self.guard.is_some());

        debug!("Flushing in evict");
        self.flush().await;

        let frame = self.guard.take().unwrap();
        frame.evict_page_owner().await.unwrap();

        debug!("Finished evicting page {}", self.pid);

        frame
    }
}

impl<'a> Deref for WritePageGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.guard
            .deref()
            .as_ref()
            .expect("Somehow have a WritePageGuard without an owned frame")
    }
}

impl<'a> DerefMut for WritePageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .deref_mut()
            .as_mut()
            .expect("Somehow have a WritePageGuard without an owned frame")
    }
}

impl<'a> Drop for WritePageGuard<'a> {
    fn drop(&mut self) {
        debug!("Dropping WritePageGuard {}", self.pid);
    }
}
