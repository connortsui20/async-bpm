//! This module contains the declaration and implementation of the [`BufferPoolManager`] type.
//!
//! TODO

use crate::{
    disk::{
        disk_manager::{DiskManager, DiskManagerHandle},
        frame::{Frame, FrameGroup, FrameGroupRef, FrameRef, FRAME_GROUP_SIZE},
    },
    page::{Page, PageHandle, PageId, PageRef, PAGE_SIZE},
};
use core::slice;
use rand::Rng;
use send_wrapper::SendWrapper;
use std::{collections::HashMap, io::IoSliceMut, rc::Rc, sync::Arc};
use tokio::{
    runtime::{Builder, Runtime},
    sync::RwLock,
};
use tracing::{debug, info, trace};

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pub(crate) pages: RwLock<HashMap<PageId, PageRef>>,

    /// Groups of frames used to demarcate eviction zones.
    pub(crate) frame_groups: Box<[FrameGroupRef]>,

    /// The manager of reading and writing [`Page`] data via [`Frame`]s.
    pub(crate) disk_manager: Arc<DiskManager>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of [`PAGE_SIZE`]ed buffer frames.
    ///
    /// The argument `capacity` should be the starting number of logical pages the user of the
    /// [`BufferPoolManager`] wishes to use, as it will allocate enough space on disk to initially
    /// accommodate that number.
    ///
    /// This function will create two copies of the buffers allocated, 1 copy for user access
    /// through [`Frame`]s and [`FrameGroup`]s, and another copy for kernel access by registering
    /// the buffers into the `io_uring` instance via
    /// [`register_buffers`](io_uring::Submitter::register_buffers).
    ///
    /// # Panics
    ///
    /// This function will panic if `num_frames` is not a multiple of [`FRAME_GROUP_SIZE`].
    pub fn new(num_frames: usize, capacity: usize) -> Self {
        assert_eq!(num_frames % FRAME_GROUP_SIZE, 0);

        // Allocate all of the buffer memory up front
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

        let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        assert_eq!(slices.len(), num_frames);

        // Create two copies of a vector of `IoSliceMut` buffer pointers
        let (registerable_buffers, buffers): (Vec<_>, Vec<_>) = slices
            .into_iter()
            .map(|buf| {
                // Safety: Since these buffers are only accessed under mutual exclusion and are
                // never accessed when the kernel has ownership over the `Frames`, this is safe to
                // create and register into `io_uring` instances.
                let register_slice =
                    unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr(), PAGE_SIZE) };

                (IoSliceMut::new(register_slice), IoSliceMut::new(buf))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unzip();

        // This copy will only be used to register into the `io_uring` instance, and never accessed
        let registerable_buffers = registerable_buffers.into_boxed_slice();

        // Create the owned frames that this buffer pool will actually manage
        let frames: Vec<FrameRef> = buffers
            .into_iter()
            .map(|buf| Arc::new(Frame::new(buf)))
            .collect();

        // Separate the owned frames into fixed-sized groups
        let frame_groups = frames
            .chunks_exact(FRAME_GROUP_SIZE)
            .map(|chunk| {
                assert_eq!(chunk.len(), FRAME_GROUP_SIZE);

                // Construct a frame array from the fixed-size slice
                let frame_array = arrayref::array_ref!(chunk, 0, FRAME_GROUP_SIZE).clone();

                Arc::new(FrameGroup {
                    frames: frame_array,
                    free_frames: async_channel::bounded(FRAME_GROUP_SIZE),
                })
            })
            .collect::<Vec<FrameGroupRef>>()
            .into_boxed_slice();
        assert_eq!(frame_groups.len(), num_frames / FRAME_GROUP_SIZE);

        let disk_manager = Arc::new(DiskManager::new(
            capacity,
            "db.test".to_string(), // TODO replace file name
            registerable_buffers,
        ));

        let pages = RwLock::new(HashMap::with_capacity(num_frames));

        Self {
            num_frames,
            frame_groups,
            disk_manager,
            pages,
        }
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Returns a pointer to a random group of frames.
    ///
    /// Intended for use by an eviction algorithm.
    pub(crate) fn get_random_frame_group(&self) -> FrameGroupRef {
        let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
        let index = rng.gen_range(0..self.frame_groups.len());

        self.frame_groups[index].clone()
    }

    /// Creates a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page already exists, this function will return that instead.
    async fn create_page(self: &Arc<Self>, pid: &PageId) -> PageHandle {
        info!("Creating {} Handle", pid);

        // First check if it exists already
        let mut pages_guard = self.pages.write().await;
        if let Some(page) = pages_guard.get(pid) {
            return PageHandle::new(page.clone(), self.disk_manager.create_handle());
        }

        // Create the new page and update the global map of pages
        let page = Arc::new(Page {
            pid: *pid,
            inner: RwLock::new(None),
            bpm: self.clone(),
        });

        pages_guard.insert(*pid, page.clone());

        // Create the page handle and return
        PageHandle::new(page, self.disk_manager.create_handle())
    }

    /// Gets a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page does not already exist, this function will create it and then return it.
    pub async fn get_page(self: &Arc<Self>, pid: &PageId) -> PageHandle {
        debug!("Getting {} Handle", pid);

        let pages_guard = self.pages.read().await;

        // Get the page if it exists, otherwise create it and return
        let page = match pages_guard.get(pid) {
            Some(page) => page.clone(),
            None => {
                drop(pages_guard);
                return self.create_page(pid).await;
            }
        };

        PageHandle::new(page, self.disk_manager.create_handle())
    }

    /// Creates a thread-local [`DiskManagerHandle`] to the inner [`DiskManager`].
    pub fn get_disk_manager(&self) -> DiskManagerHandle {
        self.disk_manager.create_handle()
    }

    /// TODO
    pub fn build_thread_runtime(self: &Arc<Self>) -> Runtime {
        let dmh = self.get_disk_manager();
        let uring = Rc::new(dmh.get_uring());
        let uring_daemon = SendWrapper::new(uring.clone());

        Builder::new_current_thread()
            .on_thread_park(move || {
                trace!("Thread parking");
                uring_daemon
                    .submit()
                    .expect("Was unable to submit `io_uring` operations");
                uring_daemon.poll();
            })
            .enable_all()
            .build()
            .unwrap()
    }
}
