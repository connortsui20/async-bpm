use crate::page::{PageId, PAGE_SIZE};
use crate::{page::PageHandle, replacer::Replacer, storage::Frame};
use async_channel::{Receiver, Sender};
use scc::Queue;
use std::{collections::HashMap, io::Result};
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

pub struct BufferPoolManager<R> {
    pub(crate) pages: Mutex<HashMap<PageId, Arc<RwLock<Option<Frame>>>>>,

    pub(crate) free_list: (Sender<Frame>, Receiver<Frame>),

    pub(crate) replacer: R,

    pub(crate) free_pages: Queue<PageId>,

    pub(crate) next_page: AtomicUsize,
}

impl<R: Replacer> BufferPoolManager<R> {
    pub fn new(num_frames: usize) -> Self {
        // Allocate all of the buffer memory up front and initialize to 0s.
        let bytes: &'static mut [u8] = vec![0u8; num_frames * PAGE_SIZE].leak();

        // Divide the memory up into `PAGE_SIZE` chunks.
        let buffers: Vec<&'static mut [u8]> = bytes.chunks_exact_mut(PAGE_SIZE).collect();
        debug_assert_eq!(buffers.len(), num_frames);

        let frames = buffers
            .into_iter()
            .enumerate()
            .map(|(i, buf)| Frame::new(i, buf));

        let (tx, rx) = async_channel::bounded(num_frames);
        for frame in frames {
            let send_res = tx.send_blocking(frame);

            debug_assert!(send_res.is_ok(), "There cannot be too many frames sent");
        }

        let pages = Mutex::new(HashMap::with_capacity(num_frames * 2));

        let replacer = R::new(num_frames);

        let free_pages = Queue::default();

        let next_page = AtomicUsize::new(0);

        Self {
            pages,
            free_list: (tx, rx),
            replacer,
            free_pages,
            next_page,
        }
    }

    pub async fn new_page(self: Arc<Self>) -> Result<PageHandle<R>> {
        let pid = self.allocate_page().await;
        Self::get_page(self, pid).await
    }

    pub async fn allocate_page(&self) -> PageId {
        match self.free_pages.pop().map(|e| **e) {
            Some(page) => page,
            None => self.next_page.fetch_add(1, Ordering::AcqRel),
        }
    }

    pub async fn deallocate_page(&self, pid: PageId) -> Result<()> {
        // TODO
        let mut guard = self.pages.lock().await;

        // If the page is not in the bpm, return Ok.
        let Some(handle) = guard.get(&pid) else {
            return Ok(());
        };

        // If the page is in the bpm and pinned, return Err.
        let pin_count = Arc::strong_count(handle);
        if pin_count > 1 {
            return Err(std::io::Error::other("Page is pinned"));
        }
        debug_assert_eq!(pin_count, 1);

        // Remove the page from the page table.
        let Some(handle) = guard.remove(&pid) else {
            unreachable!("We checked that this was present above");
        };

        if self.replacer.remove(pid).is_err() {
            unreachable!("Page in page table was somehow not in replacer");
        }

        let Some(frame) = handle.write().await.take() else {
            unreachable!("Page somehow had no frame");
        };

        if (self.free_list.0.send(frame).await).is_err() {
            unreachable!("Free list cannot become full")
        }

        // Add the PageId to the queue of free page ids.
        self.free_pages.push(pid);

        Ok(())
    }

    /// Gets a PageHandle by bringing the page data into memory and pinning it.
    pub async fn get_page(self: Arc<Self>, pid: PageId) -> Result<PageHandle<R>> {
        let mut guard = self.pages.lock().await;

        let handle = guard
            .entry(pid)
            .or_insert_with(|| Arc::new(RwLock::new(None)))
            .clone();

        let mut write_guard = handle.write().await;

        if let Some(frame) = write_guard.deref() {
            return Ok(PageHandle::new(
                pid,
                frame.id(),
                handle.clone(),
                self.clone(),
            ));
        }

        self.load(pid, &mut write_guard).await?;

        match write_guard.deref() {
            None => unreachable!("We just loaded in a Frame"),
            Some(frame) => Ok(PageHandle::new(
                pid,
                frame.id(),
                handle.clone(),
                self.clone(),
            )),
        }
    }

    async fn load(
        &self,
        pid: PageId,
        guard: &mut RwLockWriteGuard<'_, Option<Frame>>,
    ) -> Result<()> {
        // If someone else got in front of us and loaded the page for us.
        if guard.deref().deref().is_some() {
            return Ok(());
        }

        let frame = self.get_free_frame().await?;

        // TODO read in the data for this page

        self.replacer.add(frame.id());

        // Give ownership of the frame to the actual page.
        let old: Option<Frame> = guard.replace(frame);
        debug_assert!(old.is_none());

        Ok(())
    }

    async fn get_free_frame(&self) -> Result<Frame> {
        loop {
            if let Ok(frame) = self.free_list.1.try_recv() {
                return Ok(frame);
            }

            let Some(pid) = self.replacer.evict() else {
                // TODO Use a condition variable.
                continue;
            };

            let frame_handle = {
                let guard = self.pages.lock().await;

                let Some(handle) = guard.get(&pid) else {
                    unreachable!("Page in replacer was somehow not in the page table");
                };

                handle.clone()
            };

            let mut write_guard = frame_handle.write().await;

            let frame = match write_guard.take() {
                None => unreachable!("Page somehow had no frame"),
                Some(frame) => frame,
            };

            // TODO write out the frame to disk if the dirty flag is set.

            if self.free_list.0.send(frame).await.is_err() {
                unreachable!("Free list cannot become full")
            }
        }
    }
}
