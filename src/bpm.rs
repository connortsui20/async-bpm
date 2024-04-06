use crate::{
    disk::frame::Frame,
    io::IoUringAsync,
    page::{
        eviction::{Temperature, TemperatureState},
        Page, PageHandle, PageId, PageRef, PAGE_SIZE,
    },
};
use async_channel::{Receiver, Sender};
use libc::iovec;
use send_wrapper::SendWrapper;
use std::{collections::HashMap, io::IoSlice, ops::Deref, sync::Arc};
use thread_local::ThreadLocal;
use tokio::sync::RwLock;

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
pub struct BufferPoolManager {
    /// The total number of buffer frames this Buffer Pool Manager manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,

    /// A channel of free, owned buffer [`Frame`]s.
    pub(crate) free_frames: (Sender<Frame>, Receiver<Frame>),

    /// A slice of buffers, used solely to register into new [`IoUringAsync`] instances.
    io_slices: &'static [IoSlice<'static>],

    /// Thread-local `IoUringAsync` instances.
    io_urings: ThreadLocal<SendWrapper<IoUringAsync>>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of `PAGE_SIZE`ed buffer frames.
    pub fn new(num_frames: usize) -> Self {
        // All frames start out as free
        let (tx, rx) = async_channel::bounded(num_frames);

        // Allocate all of the memory up front and leak
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();
        println!(
            "Size of leaked buffers: {:#010X}",
            std::mem::size_of_val(bytes)
        );
        debug_assert_eq!(bytes.len(), num_frames * PAGE_SIZE);

        // Note: use `as_chunks_unchecked_mut()` when it is stabilized
        // https://doc.rust-lang.org/std/primitive.slice.html#method.as_chunks_unchecked_mut
        let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        debug_assert_eq!(slices.len(), num_frames);

        let buffers: Vec<IoSlice<'static>> =
            slices.into_iter().map(|buf| IoSlice::new(buf)).collect();

        for buf in buffers.iter().copied() {
            let frame = Frame::new(buf);
            tx.send_blocking(frame)
                .expect("Was unable to send the initial frames onto the global free list");
        }

        let io_slices: &'static [IoSlice] = buffers.leak();
        assert_eq!(io_slices.len(), num_frames);
        println!(
            "Size of leaked vector of IoSlice: {:#010X}",
            std::mem::size_of_val(io_slices)
        );

        Self {
            num_frames,
            free_frames: (tx, rx),
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            io_slices,
            io_urings: ThreadLocal::with_capacity(num_frames),
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// TODO docs
    ///
    /// Probably don't want to expose this anyways and use a disk manager.
    pub fn get_thread_local_uring(&self) -> IoUringAsync {
        match self.io_urings.get() {
            Some(uring) => uring.deref().clone(),
            None => {
                let mut uring =
                    IoUringAsync::try_default().expect("Unable to create an `IoUring` instance");

                self.register_buffers(&mut uring);

                self.io_urings
                    .get_or(|| SendWrapper::new(uring))
                    .deref()
                    .clone()
            }
        }
    }

    /// TODO docs
    ///
    /// Probably don't want to expose this anyways and use a disk manager.
    fn register_buffers(&self, uring: &mut IoUringAsync) {
        let ptr = self.io_slices.as_ptr() as *const iovec;

        // TODO safety
        let raw_buffers: &'static [iovec] =
            unsafe { std::slice::from_raw_parts(ptr, self.io_slices.len()) };
        assert_eq!(raw_buffers.len(), self.num_frames());

        let raw_uring = uring.uring.borrow_mut();

        // Safety: Since the slice came from `io_slices`, which has a fully `'static` lifetime in
        // both the slice of buffers and the buffers themselves, and since [`IoSlice`] is ABI
        // compatible with the `iovec` type, this is safe.
        unsafe { raw_uring.submitter().register_buffers(raw_buffers) }
            .expect("Was unable to register buffers");
    }

    /// Creates a logical page in the buffer pool manager, returning `PageHandle` to the page.
    ///
    /// If the page already exists, returns `None`.
    pub async fn create_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let mut pages_guard = self.pages.write().await;

        if pages_guard.contains_key(&pid) {
            return None;
        }

        let page = Arc::new(Page {
            pid,
            eviction_state: Temperature::new(TemperatureState::Cold),
            inner: RwLock::new(None),
            bpm: self.clone(),
        });

        pages_guard.insert(pid, page.clone());

        let uring = self.get_thread_local_uring();

        Some(PageHandle { page, uring })
    }

    /// Constructs a thread-local handle to a logical page, as long as the page already exists.
    /// If it does not exist in the context of the buffer pool manager, returns `None`.
    ///
    /// If the page does not already exist, use `create_page` instead to get a `PageHandle`.
    pub async fn get_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;

        let page = pages_guard.get(&pid)?.clone();

        let uring = self.get_thread_local_uring();

        Some(PageHandle { page, uring })
    }
}
