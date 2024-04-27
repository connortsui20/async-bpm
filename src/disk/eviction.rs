//! This module contains the types used to manage eviction state for the frame eviction algorithm.

use std::sync::atomic::{AtomicU8, Ordering};

/// The type representing a [`Frame`](super::frame::Frame)'s eviction state.
#[derive(Debug)]
pub struct Temperature {
    inner: AtomicU8,
}

/// The enum representing the possible values for [`Temperature`].
///
/// The reason this is separate from the [`Temperature`] struct is because we cannot represent do
/// atomic operations on enums in Rust.
#[derive(Debug)]
#[repr(u8)]
pub enum TemperatureState {
    /// Represents a frequently / recently accessed [`Frame`](super::frame::Frame).
    Hot = 2,
    /// Represents an infrequently or old [`Frame`] that might be evicted soon.
    Cool = 1,
    /// Represents a [`Frame`] that does not hold any [`Page`](crate::page::Page)'s data.
    Cold = 0,
}

impl Temperature {
    /// Creates a new `Temperature` type for tracking eviction state.
    pub fn new(state: TemperatureState) -> Self {
        Self {
            inner: AtomicU8::new(state as u8),
        }
    }

    /// Atomically stores a [`TemperatureState`] into the [`Temperature`] type.
    pub fn store(&self, state: TemperatureState, order: Ordering) {
        self.inner.store(state as u8, order);
    }

    /// Atomically loads a [`TemperatureState`] from the [`Temperature`] type.
    pub fn load(&self, order: Ordering) -> TemperatureState {
        match self.inner.load(order) {
            0 => TemperatureState::Cold,
            1 => TemperatureState::Cool,
            2 => TemperatureState::Hot,
            _ => unreachable!("Had an invalid value inside the `Temperature` struct"),
        }
    }
}
