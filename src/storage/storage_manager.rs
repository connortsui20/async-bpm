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

use crate::page::PAGE_SIZE;
use crate::{page::PageId, storage::frame::Frame};
use std::io::Result;
use std::ops::Deref;
use std::sync::LazyLock;
use std::{rc::Rc, sync::OnceLock};
use tokio_uring::fs::File;
use tokio_uring::BufResult;

/// TODO refactor this out
pub const DATABASE_NAME: &str = "test.db";

/// The global storage manager instance.
pub(crate) static STORAGE_MANAGER: OnceLock<StorageManager> = OnceLock::new();

std::thread_local! {
    static DB_FILE: LazyLock<Rc<File>> = LazyLock::new(|| {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(DATABASE_NAME)
            .expect("Thread is unable to create a file handle");

        let uring_file = tokio_uring::fs::File::from_std(std_file);
        Rc::new(uring_file)
    });
}

/// Manages reads into and writes from `Frame`s between memory and persistent storage.
#[derive(Debug)]
pub(crate) struct StorageManager;

impl StorageManager {
    /// Creates a new shared [`StorageManager`] instance.
    ///
    /// # Panics
    ///
    /// Panics on I/O errors, or if this function is called a second time after a successful return.
    pub(crate) fn initialize(capacity: usize) {
        tokio_uring::start(async {
            let _ = tokio_uring::fs::remove_file(DATABASE_NAME).await;

            let file = File::create(DATABASE_NAME).await?;
            file.fallocate(0, (capacity * PAGE_SIZE) as u64, libc::FALLOC_FL_ZERO_RANGE)
                .await?;

            file.close().await?;
            Ok::<(), std::io::Error>(())
        })
        .expect("I/O error on initialization");

        STORAGE_MANAGER
            .set(Self)
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
    pub(crate) fn create_handle(&self) -> Result<StorageManagerHandle> {
        let file = DB_FILE.with(|f| f.deref().clone());

        Ok(StorageManagerHandle { file })
    }

    /// Retrieves the number of drives that the pages are stored on in persistent storage.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`StorageManager::initialize`].
    pub(crate) fn get_num_drives() -> usize {
        1 // This buffer pool manager currently only supports 1 drive.
    }
}

/// A thread-local handle to a [`StorageManager`].
#[derive(Debug, Clone)]
pub(crate) struct StorageManagerHandle {
    /// A shared pointer to the thread-local file handle.
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
