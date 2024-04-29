//! This module contains the types used to manage eviction state for the frame eviction algorithm.

use crate::page::PageRef;
use std::ops::Deref;
use std::sync::Mutex;

/// The type representing a [`Frame`](super::frame::Frame)'s eviction state.
#[derive(Debug)]
pub(crate) struct FrameTemperature {
    /// A mutex-protected [`TemperatureState`] enum to ensure atomic operations.
    ///
    /// We use a synchronous / blocking mutex since operations should be held for very short periods
    /// of time, and also to ensure that operations on `FrameTemperature` are no asynchronous.
    pub(crate) inner: Mutex<TemperatureState>,
}

/// The enum representing the possible values for [`Temperature`].
///
/// The reason this is separate from the [`Temperature`] struct is because we cannot represent do
/// atomic operations on enums in Rust.
#[derive(Debug)]
pub(crate) enum TemperatureState {
    /// Represents a frequently / recently accessed [`Frame`](super::frame::Frame) that currently
    /// holds a [`Page`](crate::page::Page)'s data.
    Hot(PageRef),
    /// Represents an infrequently or old [`Frame`](super::frame::Frame) that might be evicted soon,
    /// and also still currently holds a [`Page`](crate::page::Page)'s data.
    Cool(PageRef),
    /// Represents a [`Frame`](super::frame::Frame) that does not hold any
    /// [`Page`](crate::page::Page)'s data.
    Cold,
}

impl Default for FrameTemperature {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TemperatureState::Cold),
        }
    }
}

impl FrameTemperature {
    /// Updates the eviction state after this frame has been accessed.
    pub(crate) fn was_accessed(&self) {
        let mut guard = self
            .inner
            .lock()
            .expect("FrameTemperature mutex was poisoned");
        match guard.deref() {
            TemperatureState::Hot(_) => (),
            TemperatureState::Cool(page) => *guard = TemperatureState::Hot(page.clone()),
            TemperatureState::Cold => (),
        }
    }

    /// Atomically sets the temperature as [`TemperatureState::Hot`] and then stores the page that
    /// owns the [`Frame`](super::frame::Frame) into the state.
    pub(crate) fn store_owner(&self, page: PageRef) {
        let mut guard = self
            .inner
            .lock()
            .expect("FrameTemperature mutex was poisoned");
        *guard = TemperatureState::Hot(page)
    }

    /// Atomically loads the [`Page`] that owns the [`Frame`](super::frame::Frame), if that exists.
    pub(crate) fn load_owner(&self) -> Option<PageRef> {
        let guard = self
            .inner
            .lock()
            .expect("FrameTemperature mutex was poisoned");
        match guard.deref() {
            TemperatureState::Hot(page) => Some(page.clone()),
            TemperatureState::Cool(page) => Some(page.clone()),
            TemperatureState::Cold => None,
        }
    }

    /// Runs the cooling algorithm, returning a [`PageRef`] if we want to evict the page.
    pub(crate) fn cool(&self) -> Option<PageRef> {
        let mut guard = self
            .inner
            .lock()
            .expect("FrameTemperature mutex was poisoned");

        match guard.deref() {
            TemperatureState::Hot(page) => {
                *guard = TemperatureState::Cool(page.clone());
                None
            }
            TemperatureState::Cool(page) => {
                *guard = TemperatureState::Cold;
                Some(page.clone())
            }
            TemperatureState::Cold => None,
        }
    }
}
