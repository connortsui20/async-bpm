use crate::frame::Frame;
use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

// TODO implement Optimistic Read Guard

pub struct ReadPageGuard<'a> {
    guard: RwLockReadGuard<'a, Option<Frame>>,
}

impl<'a> ReadPageGuard<'a> {
    pub(crate) fn new(guard: RwLockReadGuard<'a, Option<Frame>>) -> Self {
        assert!(guard.deref().is_some());
        Self { guard }
    }
}

impl<'a> Deref for ReadPageGuard<'a> {
    type Target = Frame;

    fn deref(&self) -> &Self::Target {
        self.guard
            .deref()
            .as_ref()
            .expect("Somehow have a ReadPageGuard without an owned frame")
    }
}

pub struct WritePageGuard<'a> {
    guard: RwLockWriteGuard<'a, Option<Frame>>,
}

impl<'a> WritePageGuard<'a> {
    pub(crate) fn new(guard: RwLockWriteGuard<'a, Option<Frame>>) -> Self {
        assert!(guard.deref().is_some());
        Self { guard }
    }
}

impl<'a> Deref for WritePageGuard<'a> {
    type Target = Frame;

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
