use page::Page;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::sync::RwLock;

pub mod page;
pub mod storage;

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
}
