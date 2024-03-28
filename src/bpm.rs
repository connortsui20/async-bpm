use crate::{
    frame::Frame,
    io::IoUringAsync,
    page::{
        eviction::{Temperature, COLD},
        page_handle::PageHandle,
        Page, PageId, PageRef, PAGE_SIZE,
    },
};
use crossbeam_queue::ArrayQueue;
use std::{collections::HashMap, io::IoSlice, sync::Arc};
use tokio::sync::{Mutex, RwLock};

/// Safety: The only thing that is `!Send` in `BufferPoolManager` is the `buffers_ptr` field, and
/// since it is created from a leaked `'static` valid `Vec`, we know it is valid and can be safely
/// sent between threads.
unsafe impl Send for BufferPoolManager {}

/// Safety: The only thing that is `!Sync` in `BufferPoolManager` is the `buffers_ptr` field, and
/// since it is created from a leaked `'static` valid `Vec`, we know it is valid and can be safely
/// sent between threads.
unsafe impl Sync for BufferPoolManager {}

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    num_frames: usize,
    pub(crate) active_pages: Mutex<Vec<PageRef>>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,
    buffers_ptr: *const IoSlice<'static>,
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
            free_frames
                .push(frame)
                .expect("Was not able to add the frame to the free_frames list");
        }

        let leaked: &'static [IoSlice] = buffers.leak();
        println!(
            "Size of leaked vector of vectors: {:#010X}",
            std::mem::size_of_val(leaked)
        );
        let buffers_ptr = leaked.as_ptr();

        Self {
            num_frames,
            active_pages: Mutex::new(Vec::with_capacity(num_frames)),
            free_frames,
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
            buffers_ptr,
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Creates a logical page in the buffer pool manager, returning `PageHandle` to the page.
    ///
    /// If the page already exists, returns `None`.
    pub async fn create_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let uring = IoUringAsync::try_default().ok()?;

        let mut pages_guard = self.pages.write().await;

        if pages_guard.contains_key(&pid) {
            return None;
        }

        let new_page = Arc::new(Page {
            pid,
            eviction_state: Temperature::new(COLD),
            inner: RwLock::new(None),
            bpm: self.clone(),
        });

        pages_guard.insert(pid, new_page.clone());

        Some(PageHandle {
            page: new_page,
            uring,
        })
    }

    /// Constructs a thread-local handle to a logical page, as long as the page already exists.
    /// If it does not exist in the context of the buffer pool manager, returns `None`.
    ///
    /// If the page does not already exist, use `create_page` instead to get a `PageHandle`.
    pub async fn get_page(self: &Arc<Self>, pid: PageId) -> Option<PageHandle> {
        let pages_guard = self.pages.read().await;

        let page = pages_guard.get(&pid)?.clone();

        // TODO make this thread local with registered buffers
        let uring = IoUringAsync::try_default().ok()?;

        Some(PageHandle { page, uring })
    }
}

#[tokio::test]
async fn test_new_bpm() {
    let num_frames = 1 << 22;
    let bpm = Arc::new(BufferPoolManager::new(num_frames));

    assert_eq!(bpm.num_frames(), num_frames);

    let id1 = PageId::new(0);
    let id2 = PageId::new(42);

    assert!(bpm.get_page(id1).await.is_none());
    let page_handle1 = bpm.create_page(id1).await;
    assert!(bpm.get_page(id1).await.is_some());

    assert!(bpm.get_page(id2).await.is_none());
    let page_handle2 = bpm.create_page(id2).await;
    assert!(bpm.get_page(id2).await.is_some());
}
