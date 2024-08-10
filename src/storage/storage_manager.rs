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
use std::io::Result;
use std::{rc::Rc, sync::OnceLock};
use tokio_uring::fs::{File, OpenOptions};
use tokio_uring::BufResult;

/// TODO remove
pub const DATABASE_NAME: &str = "test.db";

/// The global storage manager instance.
pub(crate) static STORAGE_MANAGER: OnceLock<StorageManager> = OnceLock::new();

/// Manages reads into and writes from `Frame`s between memory and persistent storage.
#[derive(Debug)]
pub(crate) struct StorageManager {
    /// TODO docs
    file: String,
}

impl StorageManager {
    /// Creates a new shared [`StorageManager`] instance.
    ///
    /// # Panics
    ///
    /// Panics on I/O errors, or if this function is called a second time after a successful return.
    pub(crate) async fn initialize() {
        let sm = Self {
            file: DATABASE_NAME.to_string(),
        };

        let _ = std::fs::remove_file(DATABASE_NAME);
        let file = File::create(&sm.file).await.unwrap();
        let s = "X".repeat(1 << 20);

        let (res, _) = file.write_all_at(s.as_bytes().to_vec(), 0).await;
        res.unwrap();

        file.close().await.unwrap();

        STORAGE_MANAGER
            .set(sm)
            .expect("Tried to set the global storage manager more than once");
    }

    /// Retrieve a static reference to the global storage manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`StorageManager::initialize`].
    pub(crate) fn get() -> &'static Self {
        STORAGE_MANAGER
            .get()
            .expect("Tried to get a reference to the storage manager before it was initialized")
    }

    /// Creates a thread-local [`StorageManagerHandle`] that has a reference back to this storage
    /// manager.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to create a [`File`] to the database files on disk.
    pub(crate) async fn create_handle(&self) -> Result<StorageManagerHandle> {
        let file = Rc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.file)
                .await?,
        );

        Ok(StorageManagerHandle { file })
    }

    /// Retrieves the number of drives that the pages are stored on in persistent storage.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`StorageManager::initialize`].
    pub(crate) fn get_num_drives() -> usize {
        1 // TODO
    }
}

/// A thread-local handle to a [`StorageManager`].
///
/// TODO this might not be named appropriately
#[derive(Debug, Clone)]
pub(crate) struct StorageManagerHandle {
    /// The inner `io_uring` instance wrapped with asynchronous capabilities and methods.
    file: Rc<File>,
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
    pub(crate) async fn read_into(&self, pid: PageId, frame: Frame) -> BufResult<(), Frame> {
        self.file.read_exact_at(frame, pid.offset()).await
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
    pub(crate) async fn write_from(&self, pid: PageId, frame: Frame) -> BufResult<(), Frame> {
        self.file.write_all_at(frame, pid.offset()).await
    }
}
