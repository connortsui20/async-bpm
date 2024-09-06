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
use std::fs::File;
use std::io::Result;
use std::os::fd::AsRawFd;
use std::os::unix::prelude::FileExt;
use std::sync::OnceLock;

/// TODO refactor this out
pub const DATABASE_NAME: &str = "test.db";

/// The global storage manager instance.
pub(crate) static STORAGE_MANAGER: OnceLock<StorageManager> = OnceLock::new();

/// Manages reads into and writes from `Frame`s between memory and persistent storage.
#[derive(Debug)]
pub(crate) struct StorageManager; // {
/// TODO does this even make sense, Ans: it does not.
// file: Arc<File>,
// }

impl StorageManager {
    /// Creates a new shared [`StorageManager`] instance.
    ///
    /// # Panics
    ///
    /// Panics on I/O errors, or if this function is called a second time after a successful return.
    pub(crate) fn initialize(capacity: usize) {
        let _ = std::fs::remove_file(DATABASE_NAME);

        let file = File::create(DATABASE_NAME).expect("Couldn't create file");
        let fd = file.as_raw_fd();

        // file.fallocate(0, (capacity * PAGE_SIZE) as u64, 0);
        // SAFETY: this is safe because its just s
        unsafe {
            // libc::fallocate(fd, 0, (capacity * PAGE_SIZE) as u64, 4096);
            libc::ftruncate(fd, (capacity * PAGE_SIZE) as i64);
        }

        let sm = Self {
            // file: Arc::new(file),
        };

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
    /// TODO make this synchronous and blocking?
    ///
    /// # Errors
    ///
    /// Returns an error if unable to create a [`File`] to the database files on disk.
    pub(crate) fn create_handle(&self) -> Result<StorageManagerHandle> {
        // let file = match self.file.try_clone() {
        //     Ok(file) => Ok(StorageManagerHandle {file}),
        //     Err(e) => {
        //         return Err(e);
        // let std_file = std::fs::OpenOptions::new()
        //     .read(true)
        //     .write(true)
        //     .open(DATABASE_NAME)?;
        // self.file = Arc::new(std_file.try_clone()?);
        // std_file
        // self.file.get_or(move || file).deref().clone()
        // self.file.get_or_init(move || ).deref().clone()
        //     }
        // };
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(DATABASE_NAME)?;

        Ok(StorageManagerHandle { file: std_file })
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
/// TODO this might not be named appropriately anymore
#[derive(Debug)]
pub(crate) struct StorageManagerHandle {
    /// TODO does this even make sense
    file: File,
}

impl Clone for StorageManagerHandle {
    fn clone(&self) -> Self {
        StorageManagerHandle {
            file: self.file.try_clone().expect("Failed to clone file"),
        }
    }
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
    pub(crate) fn read_into(&self, pid: PageId, frame: Frame) -> Result<Frame> {
        match self.file.read_exact_at(frame.buf, pid.offset()) {
            Ok(_) => Ok(frame),
            Err(e) => {
                println!("{}", e);
                Err(e)
            }
        }
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
    pub(crate) fn write_from(&self, pid: PageId, frame: Frame) -> Result<Frame> {
        match self.file.write_at(frame.buf, pid.offset()) {
            Ok(_) => Ok(frame),
            Err(e) => Err(e),
        }
    }
}
