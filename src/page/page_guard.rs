//! Wrappers around `tokio`'s `RwLockReadGuard` and `RwLockWriteGuard`, dedicated for pages of data.

use crate::page::PageId;
use crate::storage::{Frame, StorageManager};
use std::io::Result;
use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A read guard for a [`Page`](super::Page)'s `Frame`, which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can only be dereferenced in read mode, but other tasks (potentially on different
/// worker threads) are allowed to read from this same page.
pub struct ReadPageGuard<'a> {
    /// The `RwLock` read guard of the optional frame, that _must_ be the [`Some`] variant.
    ///
    /// The only reason that this guard protects an `Option<Frame>` instead of just a [`Frame`] is
    /// because the [`Page`](super::Page) type may have the `None` variant.
    ///
    /// However, we guarantee through invariants that a `ReadPageGuard` can only be constructed
    /// while the [`Page`](super::Page) has ownership over a [`Frame`], and thus we can make the
    /// assumption that this is _always_ the `Some` variant that holds an owned frame.
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
            "Cannot create a ReadPageGuard for {} that does not own a Frame",
            pid
        );

        Self { guard }
    }
}

impl Deref for ReadPageGuard<'_> {
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
    /// The only reason that this guard protects an `Option<Frame>` instead of just a [`Frame`] is
    /// because the [`Page`](super::Page) type may have the `None` variant.
    ///
    /// However, we guarantee through invariants that a `WritePageGuard` can only be constructed
    /// while the [`Page`](super::Page) has ownership over a [`Frame`], and thus we can make the
    /// assumption that this is _always_ the `Some` variant that holds an owned frame.
    guard: RwLockWriteGuard<'a, Option<Frame>>,
}

impl<'a> WritePageGuard<'a> {
    /// Creates a new `WritePageGuard`.
    ///
    /// # Panics
    ///
    /// This function will panic if the `RwLockWriteGuard` holds a `None` instead of a
    /// `Some(frame)`, since we cannot have a page guard that points to nothing.
    pub(crate) fn new(pid: PageId, mut guard: RwLockWriteGuard<'a, Option<Frame>>) -> Self {
        match guard.as_mut() {
            Some(frame) => frame.set_dirty(),
            None => unreachable!("Cannot create a WritePageGuard that does not own a Frame"),
        }

        Self { pid, guard }
    }

    /// Flushes a page's data out to persistent storage.
    ///
    /// # Errors
    ///
    /// This function will return an error if it is unable to complete the write operation to a
    /// file.
    pub async fn flush(&mut self) -> Result<()> {
        // Temporarily take ownership of the frame from the guard.
        let frame = match self.guard.take() {
            Some(frame) => frame,
            None => unreachable!("WritePageGuard somehow had no Frame"),
        };

        // Write the data out to persistent storage.
        let (res, mut frame) = StorageManager::get()
            .create_handle()?
            .write_from(self.pid, frame)
            .await;
        res?;

        frame.clear_dirty();

        // Give ownership back to the guard.
        self.guard.replace(frame);

        Ok(())
    }
}

impl Deref for WritePageGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.guard
            .deref()
            .as_ref()
            .expect("Somehow have a WritePageGuard without an owned frame")
    }
}

impl DerefMut for WritePageGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .deref_mut()
            .as_mut()
            .expect("Somehow have a WritePageGuard without an owned frame")
    }
}
