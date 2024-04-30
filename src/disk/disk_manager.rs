//! This module contains the definition and implementation of both [`DiskManager`] and
//! [`DiskManagerHandle`].
//!
//! TODO

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

/// The global disk manager instance.
static DISK_MANAGER: OnceLock<DiskManager> = OnceLock::new();

/// Manages reads into and writes from `Frame`s between memory and disk.
#[derive(Debug)]
pub struct DiskManager {
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

    /// The file storing all data. While the [`DiskManager`] has ownership, it won't be closed.
    file: File,
}

impl DiskManager {
    /// Creates a new shared [`DiskManager`] instance.
    pub fn initialize(capacity: usize, io_slices: Box<[IoSliceMut<'static>]>) {
        let file_name = "db.test"; // TODO make better

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .open(file_name)
            .unwrap_or_else(|e| panic!("Failed to open file {file_name}, with error: {e}"));

        let file_size = capacity * PAGE_SIZE;
        file.set_len(file_size as u64)
            .expect("Was unable to change the length of {file_name} to {file_size}");

        let dm = Self {
            register_buffers: io_slices,
            io_urings: ThreadLocal::new(),
            file,
        };

        // Set the global disk manager instance
        DISK_MANAGER
            .set(dm)
            .expect("Tried to set the global disk pool manager more than once")
    }

    /// Retrieve a static reference to the global disk manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`DiskManager::initialize`].
    pub fn get() -> &'static Self {
        DISK_MANAGER
            .get()
            .expect("Tried to get a reference to the disk manager before it was initialized")
    }

    /// Creates a thread-local [`DiskManagerHandle`] that has a reference back to this disk manager.
    pub fn create_handle(&self) -> DiskManagerHandle {
        let uring = self.get_thread_local_uring();

        DiskManagerHandle { uring }
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

/// A thread-local handle to a [`DiskManager`] that contains an inner [`IoUringAsync`] instance.
#[derive(Debug, Clone)]
pub struct DiskManagerHandle {
    /// The inner `io_uring` instance wrapped with asynchronous capabilities and methods.
    uring: IoUringAsync,
}

impl DiskManagerHandle {
    /// Reads a page's data into a `Frame` from disk.
    pub async fn read_into(&self, pid: PageId, mut frame: Frame) -> Result<Frame, Frame> {
        let fd = Fd(DiskManager::get().file.as_raw_fd());

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

    /// Writes a page's data on a `Frame` to disk.
    pub async fn write_from(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        let fd = Fd(DiskManager::get().file.as_raw_fd());

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
