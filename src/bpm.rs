use crate::{
    frame::Frame,
    io::IoUringAsync,
    page::{
        eviction::{Temperature, TemperatureState},
        page_handle::PageHandle,
        Page, PageId, PageRef, PAGE_SIZE,
    },
};
use crossbeam_queue::ArrayQueue;
use io_uring::{cqueue::Entry as CqEntry, opcode::ProvideBuffers, squeue::Entry as SqEntry};
use send_wrapper::SendWrapper;
use std::{collections::HashMap, io::IoSlice, ops::Deref, sync::Arc};
use thread_local::ThreadLocal;
use tokio::sync::{Mutex, RwLock};

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
pub struct BufferPoolManager {
    num_frames: usize,
    pub(crate) active_pages: Mutex<Vec<PageRef>>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,
    io_slices: &'static [IoSlice<'static>],
    io_urings: ThreadLocal<SendWrapper<IoUringAsync>>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager.
    pub fn new(num_frames: usize) -> Self {
        // All frames start out as free
        let free_frames = ArrayQueue::new(num_frames);

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
            let res = free_frames.push(frame);
            if res.is_err() {
                panic!("Was not able to add the frame to the free_frames list");
            }
        }

        let io_slices: &'static [IoSlice] = buffers.leak();
        println!(
            "Size of leaked vector of IoSlice: {:#010X}",
            std::mem::size_of_val(io_slices)
        );

        Self {
            num_frames,
            active_pages: Mutex::new(Vec::with_capacity(num_frames)),
            free_frames,
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            io_slices,
            io_urings: ThreadLocal::with_capacity(num_frames),
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    // TODO docs
    async fn get_thread_local_uring(&self) -> IoUringAsync {
        match self.io_urings.get() {
            Some(uring) => uring.deref().clone(),
            None => {
                let mut uring =
                    IoUringAsync::try_default().expect("Unable to create an `IoUring` instance");

                self.register_buffers(&mut uring).await;

                self.io_urings
                    .get_or(|| SendWrapper::new(uring))
                    .deref()
                    .clone()
            }
        }
    }

    // TODO docs
    async fn register_buffers(&self, uring: &mut IoUringAsync) {
        let ptr = self.io_slices.as_ptr();
        let addr = ptr as *mut u8;

        let buffer_len: i32 = PAGE_SIZE
            .try_into()
            .expect("PAGE_SIZE should fit into a 32-bit integer");

        // TODO can only register 2^16 bytes of buffers at a time, need to expand
        let num_buffers: u16 = self
            .io_slices
            .len()
            .try_into()
            .expect("The number of registered buffers should fit into a 16-bit integer");

        // TODO figure out buffer groups
        let provide = ProvideBuffers::new(addr, buffer_len, num_buffers, 0, 0);

        // TODO reserve some u64 numbers for registering buffers
        let entry_id = 0;
        let entry: SqEntry = provide.build().user_data(entry_id);

        println!("Attempting to register buffers");

        // Safety: Since we have a valid reference to the buffers that this entry points to, and
        // since we wait for the completion of this event while we hold the reference, the buffers
        // must be valid for the entire duration of the operation.
        let cqe: CqEntry = unsafe { uring.push(entry) }.await;

        println!("Finished registering buffers");

        assert!(cqe.user_data() == entry_id);
        assert!(
            cqe.result() >= 0,
            "Registering buffers into an `io_uring` instance failed"
        );
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

        let uring = self.get_thread_local_uring().await;

        Some(PageHandle { page, uring })
    }

    /// Constructs a thread-local handle to a logical page, as long as the page already exists.
    /// If it does not exist in the context of the buffer pool manager, returns `None`.
    ///
    /// TODO figure out how to properly handle `IoUringAsync` creation failure
    ///
    /// If the page does not already exist, use `create_page` instead to get a `PageHandle`.
    pub async fn get_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;

        let page = pages_guard.get(&pid)?.clone();

        let uring = self.get_thread_local_uring().await;

        Some(PageHandle { page, uring })
    }
}
