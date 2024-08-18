use crate::replacer::Replacer;
use scc::{HashMap, Queue};
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

pub type PageId = usize;
pub type FrameId = usize;

struct Frame {
    buf: &'static mut [u8],
}

pub struct BufferPoolManager<R> {
    pages: HashMap<PageId, FrameId>,
    frames: Vec<Arc<RwLock<Frame>>>,
    free_frames: Queue<FrameId>,

    replacer: R,

    free_pages: Queue<PageId>,
    next_page: AtomicUsize,
}

pub struct ReadPageGuard;

pub struct WritePageGuard;

impl WritePageGuard {
    pub async fn flush(&self) {
        todo!()
    }
}

impl<R: Replacer> BufferPoolManager<R> {
    pub fn new(frames: usize) -> Self {
        todo!()
    }

    pub async fn allocate_page(&self) -> PageId {
        match self.free_pages.pop().map(|e| **e) {
            Some(page) => page,
            None => self.next_page.fetch_add(1, Ordering::AcqRel),
        }
    }

    pub async fn deallocate_page(&self, pid: PageId) -> Result<(), ()> {
        // If the page is not in the bpm, return Ok.

        // If the page is in the bpm and pinned, return Err.

        // Remove the page from the page table.

        // Remove the Frame from the replacer.

        // Put the Frame in the free list.

        // Reset all other page metadata.

        // Add the PageId to the queue of free page ids.
        self.free_pages.push(pid);
        todo!()
    }

    pub async fn read_page(&self, pid: PageId) -> ReadPageGuard {
        todo!()
    }

    pub async fn write_page(&self, pid: PageId) -> WritePageGuard {
        todo!()
    }
}
