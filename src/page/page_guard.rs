//! Wrappers around `tokio`'s `RwLockReadGuard` and `RwLockWriteGuard`, dedicated for pages of data.

use crate::disk::{disk_manager::DiskManagerHandle, frame::Frame};
use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

// TODO implement Optimistic Read Guard

/// A read guard for a [`Page`](super::Page)'s [`Frame`], which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can only be dereferenced in read mode, but other tasks (potentially on different
/// worker threads) are allowed to read from this same page.
#[derive(Debug)]
pub struct ReadPageGuard<'a> {
    guard: RwLockReadGuard<'a, Option<Frame>>,
}

impl<'a> ReadPageGuard<'a> {
    pub(crate) fn new(guard: RwLockReadGuard<'a, Option<Frame>>) -> Self {
        assert!(
            guard.deref().is_some(),
            "Cannot create a ReadPageGuard that does not own a Frame"
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

/// A write guard for a [`Page`](super::Page)'s [`Frame`], which pins the page's data in memory.
///
/// When this guard is dereferenced, it is guaranteed to point to valid and correct page data.
///
/// This guard can be dereferenced in both read and write mode, and no other tasks or threads can
/// access the page's data while a task has this guard.
#[derive(Debug)]
pub struct WritePageGuard<'a> {
    guard: RwLockWriteGuard<'a, Option<Frame>>,
    dm: DiskManagerHandle,
}

impl<'a> WritePageGuard<'a> {
    pub(crate) fn new(guard: RwLockWriteGuard<'a, Option<Frame>>, dm: DiskManagerHandle) -> Self {
        assert!(
            guard.deref().is_some(),
            "Cannot create a WritePageGuard that does not own a Frame"
        );

        Self { guard, dm }
    }

    pub async fn flush(&mut self) {
        if self.guard.is_none() {
            // There is nothing for us to flush
            return;
        }

        let frame = self.guard.take().unwrap();

        let pid = frame
            .owner
            .as_ref()
            .expect("WritePageGuard protects a Frame that does not have an Page Owner")
            .pid;

        // Write the data out to disk
        let frame = self
            .dm
            .write_from(pid, frame)
            .await
            .unwrap_or_else(|_| panic!("Was unable to write data from page {:?} to disk", pid));

        let res = self.guard.replace(frame);
        assert!(res.is_none());
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
