//! This module contains the type definiton of [`EvictionState`], which is used to manage per-frame
//! state used inside the buffer pool's eviction algorithm.
//!
//! TODO more docs.

use crate::page::Page;
use std::sync::Arc;

/// The enum representing the possible states that a [`Frame`] can be in with respect to the
/// eviction algorithm.
///
/// Note that these states may not necessarily be synced to the actual state of the [`Frame`]s, and
/// these only serve as hints to the eviction algorithm.
#[derive(Debug, Clone)]
pub(crate) enum EvictionState {
    /// Represents a frequently / recently accessed [`Frame`] that currently holds a [`Page`]'s
    /// data.
    Hot(Arc<Page>),
    /// Represents an infrequently or old [`Frame`] that might be evicted soon, and also still
    /// currently holds a [`Page`] data.
    Cool(Arc<Page>),
    /// Represents either a [`Frame`] that does not hold any [`Page`] data, or a [`Frame`] that has
    /// an active thread trying to evict it from memory.
    Cold,
}

impl EvictionState {
    /// Runs the cooling algorithm, returning an optional [`Page`] if we want to evict the
    /// page.
    ///
    /// If the state is [`Hot`](EvictionState::Hot), then this function cools it down to be
    /// [`Cool`](EvictionState::Cool), and if it was already [`Cool`](EvictionState::Cool), then
    /// this function does nothing. It is on the caller to deal with eviction of the
    /// [`Cool`](EvictionState::Cool) page via the [`Page`] that is returned.
    ///
    /// If the state transitions to [`Cold`](EvictionState::Cold), this function will return the
    /// [`Page`] that it used to hold.
    pub(crate) fn cool(&mut self) -> Option<Arc<Page>> {
        match self {
            Self::Hot(page) => {
                *self = Self::Cool(page.clone());
                None
            }
            Self::Cool(page) => Some(page.clone()),
            Self::Cold => None,
        }
    }
}

impl Default for EvictionState {
    fn default() -> Self {
        Self::Cold
    }
}
