//! Implementation of [`Page`], [`PageHandle`] and other related types.
//!
//! This module contains the [`Page`] type, which represents a single logical page of data that can
//! either be both in memory and on persistent storage, or solely on persistent storage.
//!
//! Users interact with these pages via the [`PageHandle`] type, which is essentially a thread-local
//! wrapper around a pointer to a [`Page`] and an `io_uring` instance (which is further encapsulated
//! by a [`StorageManagerHandle`](crate::storage::storage_manager::StorageManagerHandle)).
//!
//! Once a user has access to a [`PageHandle`], they can create a [`ReadPageGuard`] or a
//! [`WritePageGuard`] to access the inner buffer frame and data in either read-locked or
//! write-locked mode.
//!
//! Finally, this module provides other wrapper types like [`PageId`] and [`PageRef`] to facilitate
//! easy use of the [`Page`] API.

mod page_guard;
mod page_handle;
mod pagedef;

pub use page_guard::*;
pub use page_handle::*;
pub use pagedef::*;
