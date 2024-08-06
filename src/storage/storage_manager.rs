//! This module contains the definition and implementation of both [`StorageManager`] and
//! [`StorageManagerHandle`].
//!
//! The [`StorageManager`] type is intended to be an abstraction around all of the persistent /
//! non-volatile storage that the system has access to.
//!
//! This buffer pool manager is built on the assumption that any storage requests made can be
//! carried out completely in parallel, both in software and in the hardware itself. For example,
//! this buffer pool manager will operate at its best when given access to several NVMe SSDs, all
//! attached via PCIe lanes.

use crate::{page::PageId, storage::frame::Frame};
use send_wrapper::SendWrapper;
use std::{rc::Rc, sync::OnceLock};
use thread_local::ThreadLocal;
use tokio_uring::fs::File;

/// The global storage manager instance.
static STORAGE_MANAGER: OnceLock<StorageManager> = OnceLock::new();

/// Manages reads into and writes from `Frame`s between memory and persistent storage.
#[derive(Debug)]
pub struct StorageManager {
    /// The files storing all data. While the [`StorageManager`] has ownership, they won't be closed.
    pub(crate) files: ThreadLocal<SendWrapper<Rc<Vec<File>>>>,
}

impl StorageManager {
    /// Creates a new shared [`StorageManager`] instance.
    ///
    /// # Panics
    ///
    /// Panics on I/O errors, or if this function is called a second time after a successful return.
    pub fn initialize(drives: usize, capacity: usize) {
        todo!()
    }

    /// Retrieve a static reference to the global storage manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`StorageManager::initialize`].
    pub fn get() -> &'static Self {
        STORAGE_MANAGER
            .get()
            .expect("Tried to get a reference to the storage manager before it was initialized")
    }

    /// Creates a thread-local [`StorageManagerHandle`] that has a reference back to this storage
    /// manager.
    pub fn create_handle(&self) -> StorageManagerHandle {
        todo!()
    }

    /// Retrieves the number of drives that the pages are stored on in persistent storage.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`StorageManager::initialize`].
    pub fn get_num_drives() -> usize {
        1
    }
}

/// A thread-local handle to a [`StorageManager`] that contains an inner [`IoUringAsync`] instance.
#[derive(Debug, Clone)]
pub struct StorageManagerHandle {
    /// The inner `io_uring` instance wrapped with asynchronous capabilities and methods.
    files: Rc<Vec<File>>,
}

impl StorageManagerHandle {
    /// Reads a page's data into a `Frame` from persistent storage.
    ///
    /// This function takes as input a [`PageId`] that represents a unique logical page and a
    /// `Frame` to read the page's data into.
    ///
    /// Since `io_uring` gives "ownership" of the frame that we specify to the kernel (in order for
    /// the kernel to write the data into it), this function takes full ownership of the frame and
    /// then gives it back to the caller on return.
    ///
    /// # Errors
    ///
    /// On any sort of error, we still need to return the `Frame` back to the caller, so both the
    /// `Ok` and `Err` cases return the frame back.
    pub async fn read_into(&self, pid: PageId, mut frame: Frame) -> Result<Frame, Frame> {
        let file_index = pid.file_index();
        let file = &self.files[file_index];
        let offset = pid.offset();

        todo!()

        // let (res, buffer) = file.read_exact_at(frame, offset).await;
        // if let Err(_) = res {
        //     return Err(buffer);
        // }

        // Ok(buffer)
    }

    /// Writes a page's data on a `Frame` to persistent storage.
    ///
    /// This function takes as input a [`PageId`] that represents a unique logical page and a
    /// `Frame` that holds the page's new data to store on persistent storage.
    ///
    /// Since `io_uring` gives "ownership" of the frame that we specify to the kernel (in order for
    /// the kernel to write the data into it), this function takes full ownership of the frame and
    /// then gives it back to the caller on return.
    ///
    /// # Errors
    ///
    /// On any sort of error, we still need to return the `Frame` back to the caller, so both the
    /// `Ok` and `Err` cases return the frame back.
    pub async fn write_from(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        let file_index = pid.file_index();
        let file = &self.files[file_index];
        let offset = pid.offset();

        // Read up to 10 bytes
        let (res, buffer) = file.write_all_at(frame, offset).await;
        if let Err(_) = res {
            return Err(buffer);
        }

        Ok(buffer)
    }
}
