//! This module contains the declaration and implementation of the [`BufferPoolManager`] type.
//!
//! This buffer pool manager has an asynchronous implementation that is built on top of an
//! asynchronous persistent / non-volatile storage manager, which is itself built on top of the
//! Linux `io_uring` interface.
//!
//! The goal for this buffer pool manager is to exploit parallelism as much as possible by limiting
//! the use of any global latches or single points of contention for the entire system. This means
//! that several parts of the system are implemented quite differently from how a traditional buffer
//! pool manager would work.

use crate::{
    page::Page,
    storage::frame::{Frame, FrameGroup},
};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::sync::RwLock;

/// The global buffer pool manager instance.
static BPM: OnceLock<BufferPoolManager> = OnceLock::new();

/// A parallel Buffer Pool Manager that manages bringing logical pages from persistent storage into
/// memory via shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pages: RwLock<HashMap<usize, Arc<Page>>>,

    /// TODO docs
    groups: Vec<FrameGroup>,
}
