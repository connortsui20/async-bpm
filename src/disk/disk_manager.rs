use crate::{io::IoUringAsync, page::PageId};
use libc::iovec;
use send_wrapper::SendWrapper;
use std::{io::IoSlice, ops::Deref, sync::Arc};
use thread_local::ThreadLocal;

use super::frame::Frame;

#[derive(Debug)]
pub struct DiskManager {
    /// A slice of buffers, used solely to register into new [`IoUringAsync`] instances.
    ///
    /// Right now, this only supports a single group of buffers, but in the future it should be able
    /// to support multiple groups of buffers to support more shared memory for the buffer pool.
    io_slices: &'static [IoSlice<'static>],

    /// Thread-local `IoUringAsync` instances.
    io_urings: ThreadLocal<SendWrapper<IoUringAsync>>,
}

impl DiskManager {
    pub fn new(io_slices: &'static [IoSlice<'static>]) -> Self {
        Self {
            io_slices,
            io_urings: ThreadLocal::new(),
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
    pub async fn read(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        todo!()
    }

    pub async fn write(&self, pid: PageId, frame: Frame) -> Result<Frame, Frame> {
        todo!()
    }
}
