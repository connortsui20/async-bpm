//! Wrappers around `tokio`'s `RwLockReadGuard` and `RwLockWriteGuard`, dedicated for pages of data.

use super::PageId;
use crate::disk::{disk_manager::DiskManager, frame::Frame};
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
    /// The unique page ID of the page this guard read protects. TODO Does not have a use yet...
    _pid: PageId,

    /// The `RwLock` read guard of the optional frame, that _must_ be the [`Some`] variant.
    ///
    /// The only reason that this guard protects an `Option<Frame>` instead of a `Frame` is because
    /// the [`Page`](super::Page) type may have the `None` variant. However, _we_ guarantee through
    /// panics that a `ReadPageGuard` can only be constructed while the [`Page`](super::Page) has
    /// ownership over a `Frame`, and thus can make the assumption that this is _always_ the `Some`
    /// variant that holds an owned frame.
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

        debug!("Created ReadPageGuard of {}", pid);

        Self { _pid: pid, guard }
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

/// A write guard for a [`Page`](super::Page)'s `Frame`, which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can be dereferenced in both read and write mode, and no other tasks or threads can
/// access the page's data while a task has this guard.
pub struct WritePageGuard<'a> {
    /// The unique page ID of the page this guard read protects.
    pid: PageId,

    /// The `RwLock` write guard of the optional frame, that _must_ be the [`Some`] variant.
    ///
    /// The only reason that this guard protects an `Option<Frame>` instead of a `Frame` is because
    /// the [`Page`](super::Page) type may have the `None` variant. However, _we_ guarantee through
    /// panics that a `WritePageGuard` can only be constructed while the [`Page`](super::Page) has
    /// ownership over a `Frame`, and thus can make the assumption that this is _always_ the `Some`
    /// variant that holds an owned frame.
    guard: RwLockWriteGuard<'a, Option<Frame>>,
}

impl<'a> WritePageGuard<'a> {
    /// Creates a new `WritePageGuard`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `RwLockWriteGuard` holds a `None` instead of a
    /// `Some(frame)`, since we cannot have a page guard that points to nothing.
    pub(crate) fn new(pid: PageId, guard: RwLockWriteGuard<'a, Option<Frame>>) -> Self {
        assert!(
            guard.is_some(),
            "Cannot create a WritePageGuard that does not own a Frame"
        );

        debug!("Created WritePageGuard of {}", pid);

        Self { pid, guard }
    }

    /// Flushes a page's data out to disk.
    pub async fn flush(&mut self) {
        assert!(self.guard.is_some());

        // Temporarily take ownership of the frame from the guard
        let frame = self.guard.take().unwrap();

        // Write the data out to disk
        let frame = DiskManager::get()
            .create_handle()
            .write_from(self.pid, frame)
            .await
            .unwrap_or_else(|_| panic!("Was unable to write data from page {} to disk", self.pid));

        // Give ownership back to the guard
        self.guard.replace(frame);
    }

    /// Evicts the page from memory, consuming the guard in the process.
    pub(crate) async fn evict(mut self) -> Frame {
        assert!(self.guard.is_some());

        self.flush().await;

        let frame = self.guard.take().unwrap();
        frame.evict_page_owner().unwrap();

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
