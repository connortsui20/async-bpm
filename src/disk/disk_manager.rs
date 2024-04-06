use super::frame::Frame;
use crate::{
    io::IoUringAsync,
    page::{PageId, PAGE_SIZE},
};
use io_uring::{opcode, types::Fd};
use libc::{iovec, O_DIRECT};
use send_wrapper::SendWrapper;
use std::{
    fs::{File, OpenOptions},
    io::IoSlice,
    ops::Deref,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    sync::Arc,
};
use thread_local::ThreadLocal;

#[derive(Debug)]
pub struct DiskManager {
    /// A slice of buffers, used solely to register into new [`IoUringAsync`] instances.
    ///
    /// Right now, this only supports a single group of buffers, but in the future it should be able
    /// to support multiple groups of buffers to support more shared memory for the buffer pool.
    io_slices: &'static [IoSlice<'static>],

    /// Thread-local `IoUringAsync` instances.
    io_urings: ThreadLocal<SendWrapper<IoUringAsync>>,

    /// The file storing all data. While the [`DiskManager`] has ownership, it won't be closed.
    file: File,
}

impl DiskManager {
    pub fn new(
        num_initial_pages: usize,
        file_name: String,
        io_slices: &'static [IoSlice<'static>],
    ) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .open(&file_name)
            .unwrap_or_else(|e| panic!("Failed to open file {file_name}, with error: {e}"));

        let file_size = num_initial_pages * PAGE_SIZE;
        file.set_len(file_size as u64)
            .expect("Was unable to change the length of {file_name} to {file_size}");

        Self {
            io_slices,
            io_urings: ThreadLocal::new(),
            file,
        }
    }

    /// Creates a thread-local [`DiskManagerHandle`] that has a reference back to this disk manager.
    pub fn create_handle(self: &Arc<Self>) -> DiskManagerHandle {
        let uring = self.get_thread_local_uring();

        DiskManagerHandle {
            disk_manager: self.clone(),
            uring,
        }
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

        // Now register the buffers as shared between the user and the kernel
        {
            let ptr = self.io_slices.as_ptr() as *const iovec;

            // Safety: Since the pointer came from a valid slice, and since `IoSlice` is ABI
            // compatible with `iovec`, this is safe.
            let raw_buffers: &'static [iovec] =
                unsafe { std::slice::from_raw_parts(ptr, self.io_slices.len()) };

            let raw_uring = uring.uring.borrow_mut();

            // Safety: Since the slice came from `io_slices`, which has a fully `'static` lifetime in
            // both the slice of buffers and the buffers themselves, this is safe.
            unsafe { raw_uring.submitter().register_buffers(raw_buffers) }
                .expect("Was unable to register buffers");
        }

        // Install and return the new thread-local `IoUringAsync` instance
        self.io_urings
            .get_or(|| SendWrapper::new(uring))
            .deref()
            .clone()
    }
}

#[derive(Debug)]
pub struct DiskManagerHandle {
    disk_manager: Arc<DiskManager>,
    uring: IoUringAsync,
}

impl DiskManagerHandle {
    pub async fn read_into(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        let fd = Fd(self.disk_manager.file.as_raw_fd());

        // Since we own the frame (and nobody else is reading from it), this is fine to mutate
        let buf_ptr = frame.buf.as_ptr() as *mut u8;

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

    pub async fn write_from(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        let fd = Fd(self.disk_manager.file.as_raw_fd());

        let buf_ptr = frame.buf.as_ptr();

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
}