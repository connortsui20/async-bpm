use super::*;
use crate::page::PageId;
use std::sync::Mutex;

struct FifoInner {
    queue: Vec<usize>,
    pinned: Vec<usize>,
}

pub struct Fifo {
    inner: Mutex<FifoInner>,
}

impl Replacer for Fifo {
    fn new(num_frames: usize) -> Self {
        Self {
            inner: Mutex::new(FifoInner {
                queue: Vec::with_capacity(num_frames),
                pinned: Vec::new(),
            }),
        }
    }

    fn pin(&self, pid: PageId) -> Result<usize, FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        if let Some(index) = guard.queue.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            guard.pinned.push(index);
            return Ok(1);
        }

        if guard.pinned.iter().any(|&x| x == pid) {
            return Ok(1);
        }

        Err(FrameNotFound)
    }

    fn unpin(&self, pid: PageId) -> Result<usize, FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        if guard.queue.iter().any(|&x| x == pid) {
            return Ok(1);
        }

        if let Some(index) = guard.pinned.iter().position(|&x| x == pid) {
            guard.pinned.remove(index);
            guard.queue.push(index);
        }

        Err(FrameNotFound)
    }

    fn record_access(&self, pid: PageId, _access: AccessType) -> Result<(), FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        if let Some(index) = guard.queue.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            guard.queue.push(index);
            return Ok(());
        }

        if guard.pinned.iter().any(|&x| x == pid) {
            return Ok(());
        }

        Err(FrameNotFound)
    }

    fn add(&self, pid: PageId) {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        guard.pinned.push(pid);
    }

    fn evict(&self) -> Option<PageId> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        if guard.queue.is_empty() {
            None
        } else {
            Some(guard.queue.remove(0))
        }
    }

    fn remove(&self, pid: PageId) -> Result<(), FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        if let Some(index) = guard.queue.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            return Ok(());
        }

        if let Some(index) = guard.pinned.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            return Ok(());
        }

        Err(FrameNotFound)
    }

    fn size(&self) -> usize {
        let guard = self.inner.lock().expect("Lock was somehow poisoned");

        guard.queue.len() + guard.pinned.len()
    }
}
