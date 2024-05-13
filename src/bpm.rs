//! This module contains the declaration and implementation of the [`BufferPoolManager`] type.
//!
//! This buffer pool manager has an asynchronous implementation that is built on top of an
//! asynchronous disk / permanent storage manager, which is itself built on top of the Linux
//! `io_uring` interface.
//!
//! The goal for this buffer pool manager is to exploit parallelism as much as possible by limiting
//! the use of any global latches or single points of contention for the entire system. This means
//! that several parts of the system are implemented quite differently from how a traditional buffer
//! pool manager would work.

use crate::{
    drive::{
        drive_manager::{DriveManager, DriveManagerHandle},
        frame::{FrameGroup, FrameGroupRef, FRAME_GROUP_SIZE},
    },
    page::{Page, PageHandle, PageId, PageRef, PAGE_SIZE},
};
use core::slice;
use rand::Rng;
use send_wrapper::SendWrapper;
use std::{
    collections::HashMap,
    io::IoSliceMut,
    rc::Rc,
    sync::{Arc, OnceLock},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::RwLock,
};

/// The global buffer pool manager instance.
static BPM: OnceLock<BufferPoolManager> = OnceLock::new();

/// A parallel Buffer Pool Manager that manages bringing logical pages from disk into memory via
/// shared and fixed buffer frames.
#[derive(Debug)]
pub struct BufferPoolManager {
    /// The total number of buffer frames this [`BufferPoolManager`] manages.
    num_frames: usize,

    /// A mapping between unique [`PageId`]s and shared [`PageRef`] handles.
    pages: RwLock<HashMap<PageId, PageRef>>,

    /// Groups of frames used to demarcate eviction zones.
    frame_groups: Box<[FrameGroupRef]>,
}

impl BufferPoolManager {
    /// Constructs a new buffer pool manager with the given number of [`PAGE_SIZE`]ed buffer frames.
    ///
    /// The argument `capacity` should be the starting number of logical pages the user of the
    /// [`BufferPoolManager`] wishes to use, as it will allocate enough space on disk to initially
    /// accommodate that number. TODO this is subject to change once the disk manager improves.
    ///
    /// This function will create two copies of the buffers allocated, 1 copy for user access
    /// through `Frame`s and `FrameGroup`s, and another copy for kernel access by registering
    /// the buffers into the `io_uring` instance via
    /// [`register_buffers`](io_uring::Submitter::register_buffers).
    ///
    /// # Panics
    ///
    /// This function will panic if `num_frames` is not a multiple of
    /// [`FRAME_GROUP_SIZE`]((crate::disk::frame::FRAME_GROUP_SIZE)).
    pub fn initialize(num_frames: usize, capacity: usize) {
        assert!(
            BPM.get().is_none(),
            "Tried to initialize a BufferPoolManager more than once"
        );
        assert!(num_frames != 0);
        assert_eq!(num_frames % FRAME_GROUP_SIZE, 0);
        let num_groups = num_frames / FRAME_GROUP_SIZE;

        // Allocate all of the buffer memory up front
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

        // Divide the memory up into `PAGE_SIZE` chunks
        let slices: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        assert_eq!(slices.len(), num_frames);

        // Create two copies of a vector of `IoSliceMut` buffer pointers
        let (mut buffers, registerable_buffers): (Vec<_>, Vec<_>) = slices
            .into_iter()
            .map(|buf| {
                // Safety: Since these buffers are only accessed under mutual exclusion and are
                // never accessed when the kernel has ownership over the `Frames`, this is safe to
                // create and register into `io_uring` instances.
                let register_slice =
                    unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr(), PAGE_SIZE) };

                (buf, IoSliceMut::new(register_slice))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unzip();
        assert_eq!(buffers.len(), num_frames);

        // Create the frame groups, taking the groups of buffers off the back of the buffers vector
        let frame_groups: Vec<FrameGroupRef> = (0..num_groups)
            .map(|i| {
                let buffers: Vec<&'static mut [u8]> =
                    buffers.split_off(buffers.len() - FRAME_GROUP_SIZE);
                assert_eq!(buffers.len(), FRAME_GROUP_SIZE, "Not equal at index {i}");

                FrameGroup::new(buffers, num_groups - i - 1)
            })
            .collect();
        assert_eq!(frame_groups.len(), num_frames / FRAME_GROUP_SIZE);
        assert!(
            buffers.is_empty(),
            "All buffers should have been moved into frame groups"
        );

        // Create the bpm and set it as the global static bpm instance
        BPM.set(Self {
            num_frames,
            frame_groups: frame_groups.into_boxed_slice(),
            pages: RwLock::new(HashMap::with_capacity(num_frames)),
        })
        .expect("Tried to initialize the buffer pool manager more than once");

        // This copy will only be used to register into the `io_uring` instance, and never accessed
        let registerable_buffers = registerable_buffers.into_boxed_slice();

        // Initialize the global `DiskManager` instance
        DriveManager::initialize(8, capacity, registerable_buffers);
    }

    /// Retrieve a static reference to the global buffer pool manager.
    ///
    /// # Panics
    ///
    /// This function will panic if it is called before a call to [`BufferPoolManager::initialize`].
    pub fn get() -> &'static Self {
        BPM.get()
            .expect("Tried to get a reference to the BPM before it was initialized")
    }

    /// Gets the number of fixed frames the buffer pool manages.
    pub fn num_frames(&self) -> usize {
        self.num_frames
    }

    /// Returns a pointer to a random group of frames.
    ///
    /// Intended for use by an eviction algorithm.
    pub(crate) fn get_random_frame_group(&self) -> FrameGroupRef {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.frame_groups.len());

        self.frame_groups[index].clone()
    }

    /// Creates a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page already exists, this function will return that instead.
    async fn create_page(&self, pid: &PageId) -> PageHandle {
        // First check if it exists already
        let mut pages_guard = self.pages.write().await;
        if let Some(page) = pages_guard.get(pid) {
            return PageHandle::new(page.clone(), DriveManager::get().create_handle());
        }

        // Create the new page and update the global map of pages
        let page = Arc::new(Page {
            pid: *pid,
            inner: RwLock::new(None),
        });

        pages_guard.insert(*pid, page.clone());

        // Create the page handle and return
        PageHandle::new(page, DriveManager::get().create_handle())
    }

    /// Gets a thread-local page handle of the buffer pool manager, returning a [`PageHandle`] to
    /// the logical page data.
    ///
    /// If the page does not already exist, this function will create it and then return it.
    pub async fn get_page(&self, pid: &PageId) -> PageHandle {
        let pages_guard = self.pages.read().await;

        // Get the page if it exists, otherwise create it and return
        let page = match pages_guard.get(pid) {
            Some(page) => page.clone(),
            None => {
                drop(pages_guard);
                return self.create_page(pid).await;
            }
        };

        PageHandle::new(page, DriveManager::get().create_handle())
    }

    /// Creates a thread-local [`DiskManagerHandle`] to the inner [`DiskManager`].
    pub fn get_disk_manager(&self) -> DriveManagerHandle {
        DriveManager::get().create_handle()
    }

    /// Creates a `tokio` thread-local [`Runtime`] that works with
    /// [`IoUringAsync`](crate::io::IoUringAsync) by calling `submit` and `poll` every time a worker
    /// thread gets parked.
    ///
    /// # Panics
    ///
    /// This function will panic if it is unable to build the [`Runtime`].
    pub fn build_thread_runtime(&self) -> Runtime {
        let dmh = self.get_disk_manager();
        let uring = Rc::new(dmh.get_uring());
        let uring_daemon = SendWrapper::new(uring.clone());

        Builder::new_current_thread()
            .on_thread_park(move || {
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
