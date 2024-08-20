//! TODO Wrappers around `tokio`'s `RwLockReadGuard` and `RwLockWriteGuard`, dedicated for pages of
//! data.

use crate::frame::Frame;
use crate::replacer::{AccessType, Replacer};
use crate::{BufferPoolManager, PageId};
use std::io::Result;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct PageHandle<R: Replacer> {
    pid: PageId,
    frame_id: usize,
    frame: Arc<RwLock<Option<Frame>>>,
    bpm: Arc<BufferPoolManager<R>>,
}

impl<R: Replacer> PageHandle<R> {
    pub(crate) fn new(
        pid: PageId,
        frame_id: usize,
        frame: Arc<RwLock<Option<Frame>>>,
        bpm: Arc<BufferPoolManager<R>>,
    ) -> Self {
        Self {
            pid,
            frame_id,
            frame,
            bpm,
        }
    }

    pub async fn read_page(&self) -> ReadPageGuard {
        let read_guard = self.frame.read().await;

        match read_guard.deref() {
            None => unreachable!("PageHandle somehow had no frame"),
            Some(frame) => {
                self.bpm
                    .replacer
                    .record_access(frame.id(), AccessType::Unknown)
                    .expect("TODO");
                ReadPageGuard::new(self.pid, read_guard)
            }
        }
    }

    pub async fn write_page(&self) -> WritePageGuard {
        let write_guard = self.frame.write().await;

        match write_guard.deref() {
            None => unreachable!("PageHandle somehow had no frame"),
            Some(frame) => {
                self.bpm
                    .replacer
                    .record_access(frame.id(), AccessType::Unknown)
                    .expect("TODO");
                WritePageGuard::new(self.pid, write_guard)
            }
        }
    }
}

impl<R: Replacer> Drop for PageHandle<R> {
    fn drop(&mut self) {
        let pin_count = match self.bpm.replacer.unpin(self.frame_id) {
            Err(_) => unreachable!("The frame must be present since we have a page handle"),
            Ok(count) => count,
        };

        debug_assert_eq!(pin_count, Arc::strong_count(&self.frame));

        if Arc::strong_count(&self.frame) == 1 {
            todo!()
        }
    }
}

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
    pub(crate) fn new(pid: PageId, guard: RwLockWriteGuard<'a, Option<Frame>>) -> Self {
        assert!(
            guard.deref().is_some(),
            "Cannot create a WritePageGuard for {} that does not own a Frame",
            pid
        );

        Self { guard }
    }

    /// Flushes a page's data out to persistent storage.
    ///
    /// # Errors
    ///
    /// This function will return an error if it is unable to complete the write operation to a
    /// file.
    pub async fn flush(&mut self) -> Result<()> {
        debug_assert!(self.guard.is_some());

        todo!()
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
