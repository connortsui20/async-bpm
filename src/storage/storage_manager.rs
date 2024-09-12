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
use std::fs::File;
use std::io::Result;
use std::ops::Deref;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::prelude::FileExt;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, OnceLock};

/// TODO refactor this out
pub const DATABASE_NAME: &str = "bpm.db";

/// The global storage manager instance.
pub(crate) static STORAGE_MANAGER: OnceLock<StorageManager> = OnceLock::new();

/// The total number of I/O operations.
pub static IO_OPERATIONS: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    static DB_FILE: LazyLock<Rc<File>> = LazyLock::new(|| {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(DATABASE_NAME)
            .expect("Thread is unable to create a file handle");

        Rc::new(std_file)
    });
}

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
    pub(crate) fn initialize(_capacity: usize) {
        // let _ = std::fs::remove_file(DATABASE_NAME);

        // let file = File::create(DATABASE_NAME).expect("Couldn't create file");
        // let fd = file.as_raw_fd();

        // file.fallocate(0, (capacity * PAGE_SIZE) as u64, 0);
        // SAFETY: this is safe because its just s
        // unsafe {
        //     // libc::fallocate(fd, 0, (capacity * PAGE_SIZE) as u64, 4096);
        //     libc::ftruncate(fd, (capacity * PAGE_SIZE) as i64);
        // }

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
        let std_file = DB_FILE.with(|f| f.deref().clone());

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
    file: Rc<File>,
}

impl Clone for StorageManagerHandle {
    fn clone(&self) -> Self {
        StorageManagerHandle {
            file: self.file.clone(),
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
        IO_OPERATIONS.fetch_add(1, Ordering::Relaxed);
        match self.file.read_exact_at(frame.buf, pid.offset()) {
            Ok(_) => Ok(frame),
            Err(e) => Err(e),
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
        IO_OPERATIONS.fetch_add(1, Ordering::Relaxed);
        match self.file.write_at(frame.buf, pid.offset()) {
            Ok(_) => Ok(frame),
            Err(e) => Err(e),
        }
    }
}
