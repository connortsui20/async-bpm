//! This module contains the definition and implementation of both [`DriveManager`] and
//! [`DriveManagerHandle`].
//!
//! The [`DriveManager`] type is intended to be an abstraction around all of the persistent /
//! non-volatile storage that the system has access to.
//!
//! This buffer pool manager is built on the assumption that any storage requests made can be
//! carried out completely in parallel, both in software and in the hardware itself. For example,
//! this buffer pool manager will operate at its best when given access to several NVMe SSDs, all
//! attached via PCIe lanes.

use super::frame::Frame;
use crate::{
    io::IoUringAsync,
    page::{PageId, PAGE_SIZE},
};
use io_uring::{opcode, types::Fd};
use libc::O_DIRECT;
use send_wrapper::SendWrapper;
use std::{
    fs::{File, OpenOptions},
    io::IoSliceMut,
    ops::Deref,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    sync::OnceLock,
};
use thread_local::ThreadLocal;

/// The global drive manager instance.
static DRIVE_MANAGER: OnceLock<DriveManager> = OnceLock::new();

/// Manages reads into and writes from `Frame`s between memory and persistent storage.
#[derive(Debug)]
pub struct DriveManager {
    /// A slice of buffers, used solely to register into new [`IoUringAsync`] instances.
    ///
    /// Right now, this only supports a single group of buffers, but in the future it should be able
    /// to support multiple groups of buffers to support more shared memory for the buffer pool.
    ///
    /// For safety purposes, we cannot ever read from any of these slices, as we should only be
    /// accessing the inner data through [`Frame`]s.
    register_buffers: Box<[IoSliceMut<'static>]>,

    /// Thread-local `IoUringAsync` instances.
    io_urings: ThreadLocal<SendWrapper<IoUringAsync>>,

    /// The files storing all data. While the [`DriveManager`] has ownership, they won't be closed.
    pub(crate) files: Vec<File>,
}

impl DriveManager {
    /// Creates a new shared [`DriveManager`] instance.
    ///
    /// # Panics
    ///
    /// Panics on I/O errors, or if this function is called a second time after a successful return.
    pub fn initialize(drives: usize, capacity: usize, io_slices: Box<[IoSliceMut<'static>]>) {
        let files = (0..drives)
            .map(|d| {
                let file_name = format!("bpm.dm.{}.db", d);

                // TODO these files should be on separate drives
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .custom_flags(O_DIRECT)
                    .open(&file_name)
                    .unwrap_or_else(|e| panic!("Failed to open file {file_name}, with error: {e}"));

                let file_size = (capacity / drives) * PAGE_SIZE;
                file.set_len(file_size as u64)
                    .expect("Was unable to change the length of {file_name} to {file_size}");

                file
            })
            .collect();

        let dm = Self {
            register_buffers: io_slices,
            io_urings: ThreadLocal::new(),
            files,
        };

        // Set the global drive manager instance
        DRIVE_MANAGER
            .set(dm)
            .expect("Tried to set the global drive manager more than once")
    }

    /// Retrieve a static reference to the global drive manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`DriveManager::initialize`].
    pub fn get() -> &'static Self {
        DRIVE_MANAGER
            .get()
            .expect("Tried to get a reference to the drive manager before it was initialized")
    }

    /// Retrieves the number of drives that the pages are stored on.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`DriveManager::initialize`].
    pub fn get_num_drives() -> usize {
        Self::get().files.len()
    }

    /// Creates a thread-local [`DriveManagerHandle`] that has a reference back to this drive manager.
    pub fn create_handle(&self) -> DriveManagerHandle {
        let uring = self.get_thread_local_uring();

        DriveManagerHandle { uring }
    }

    /// A helper function that either retrieves the already-created thread-local [`IoUringAsync`]
    /// instance, or creates a new one and returns that.
    ///
    /// The [`IoUringAsync`] instance will have the buffers pre-registered.
    fn get_thread_local_uring(&self) -> IoUringAsync {
        // If it already exists, we don't need to make a new one
        if let Some(uring) = self.io_urings.get() {
            return uring.deref().clone();
        }

        // Construct the new `IoUringAsync` instance
        let uring = IoUringAsync::try_default().expect("Unable to create an `IoUring` instance");

        // TODO this doesn't work yet
        std::hint::black_box(&self.register_buffers);
        // uring.register_buffers(&self.register_buffers);

        // Install and return the new thread-local `IoUringAsync` instance
        self.io_urings
            .get_or(|| SendWrapper::new(uring))
            .deref()
            .clone()
    }
}

/// A thread-local handle to a [`DriveManager`] that contains an inner [`IoUringAsync`] instance.
#[derive(Debug, Clone)]
pub struct DriveManagerHandle {
    /// The inner `io_uring` instance wrapped with asynchronous capabilities and methods.
    uring: IoUringAsync,
}

impl DriveManagerHandle {
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
        let dm: &DriveManager = DriveManager::get();
        let file_index = pid.file_index();
        let fd = Fd(dm.files[file_index].as_raw_fd());

        // Since we own the frame (and nobody else is reading from it), this is fine to mutate
        let buf_ptr = frame.as_mut_ptr();

        let entry = opcode::Read::new(fd, buf_ptr, PAGE_SIZE as u32)
            .offset(pid.offset())
            .build()
            .user_data(pid.as_u64());

        // Safety: Since this function owns the `Frame`, we can guarantee that the buffer the
        // `Frame` owns will be valid for the entire duration of this operation
        let cqe = unsafe { self.uring.push(entry).await };

        if cqe.result() >= 0 {
            Ok(frame)
        } else {
            Err(frame)
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
    pub async fn write_from(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        let dm: &DriveManager = DriveManager::get();
        let file_index = pid.file_index();
        let fd = Fd(dm.files[file_index].as_raw_fd());

        let buf_ptr = frame.as_ptr();

        let entry = opcode::Write::new(fd, buf_ptr, PAGE_SIZE as u32)
            .offset(pid.offset())
            .build()
            .user_data(pid.as_u64());

        // Safety: Since this function owns the `Frame`, we can guarantee that the buffer the
        // `Frame` owns will be valid for the entire duration of this operation
        let cqe = unsafe { self.uring.push(entry).await };

        if cqe.result() >= 0 {
            Ok(frame)
        } else {
            Err(frame)
        }
    }

    /// Retrieves the thread-local `io_uring` instance.
    pub fn get_uring(&self) -> IoUringAsync {
        self.uring.clone()
    }
}
