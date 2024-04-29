//! This module contains the types used to manage eviction state for the frame eviction algorithm.

use crate::page::PageRef;
use std::ops::Deref;
use tokio::sync::Mutex;

/// The type representing a [`Frame`](super::frame::Frame)'s eviction state.
#[derive(Debug)]
pub(crate) struct FrameTemperature {
    pub(crate) inner: Mutex<TemperatureState>,
}

/// The enum representing the possible values for [`Temperature`].
///
/// The reason this is separate from the [`Temperature`] struct is because we cannot represent do
/// atomic operations on enums in Rust.
#[derive(Debug)]
pub(crate) enum TemperatureState {
    /// Represents a frequently / recently accessed [`Frame`](super::frame::Frame).
    Hot(PageRef),
    /// Represents an infrequently or old [`Frame`](super::frame::Frame) that might be evicted soon.
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
    /// Atomically sets the temperature as [`TemperatureState::Hot`] and then stores the page that
    /// owns the [`Frame`](super::frame::Frame) into the state.
    pub(crate) async fn store_owner(&self, page: PageRef) {
        let mut guard = self.inner.lock().await;
        *guard = TemperatureState::Hot(page)
    }

    /// Atomically loads the [`Page`] that owns the [`Frame`](super::frame::Frame), if that exists.
    pub(crate) async fn load_owner(&self) -> Option<PageRef> {
        let guard = self.inner.lock().await;
        match guard.deref() {
            TemperatureState::Hot(page) => Some(page.clone()),
            TemperatureState::Cool(page) => Some(page.clone()),
            TemperatureState::Cold => None,
        }
    }

    /// Updates the eviction state after this frame has been accessed.
    pub(crate) async fn was_accessed(&self) {
        let mut guard = self.inner.lock().await;
        match guard.deref() {
            TemperatureState::Hot(_) => (),
            TemperatureState::Cool(page) => *guard = TemperatureState::Hot(page.clone()),
            TemperatureState::Cold => (),
        }
    }
}
