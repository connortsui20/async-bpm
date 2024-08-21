use super::*;
use crate::page::PageId;
use std::{collections::HashMap, sync::Mutex};

struct FifoInner {
    /// A queue of unpinned pages.
    queue: Vec<usize>,

    /// A map of pinned pages and their pin counts.
    pinned: HashMap<usize, usize>,
}

pub struct Fifo {
    inner: Mutex<FifoInner>,
}

impl Replacer for Fifo {
    fn new(num_frames: usize) -> Self {
        Self {
            inner: Mutex::new(FifoInner {
                queue: Vec::with_capacity(2 * num_frames),
                pinned: HashMap::new(),
            }),
        }
    }

    fn pin(&self, pid: PageId) -> Result<usize, FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        // If the page is unpinned it will be in the queue, so remove it.
        if let Some(index) = guard.queue.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            assert!(guard.pinned.insert(pid, 1).is_none());
            return Ok(1);
        }

        // Otherwise, it is already pinned so increment the pin count.
        match guard.pinned.get_mut(&pid) {
            Some(count) => {
                *count += 1;
                Ok(*count)
            }
            None => Err(FrameNotFound),
        }
    }

    fn unpin(&self, pid: PageId) -> Result<usize, FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        // The the page is already unpinned then do nothing.
        if guard.queue.iter().any(|&x| x == pid) {
            return Ok(0);
        }

        // Otherwise, it is already pinned so decrement the pin count.
        let Some(count) = guard.pinned.get_mut(&pid) else {
            return Err(FrameNotFound);
        };

        debug_assert_ne!(*count, 0);

        if *count > 1 {
            *count -= 1;
            return Ok(*count);
        }

        let count = *count;
        debug_assert_eq!(count, 1);

        // If the pin count has hit zero, add the page to the queue.
        assert!(guard.pinned.remove(&pid).is_some());
        guard.queue.push(pid);

        Ok(0)
    }

    fn record_access(&self, pid: PageId, _access: AccessType) -> Result<(), FrameNotFound> {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        // Find the page in the queue and move it to the back.
        if let Some(index) = guard.queue.iter().position(|&x| x == pid) {
            guard.queue.remove(index);
            guard.queue.push(index);
            return Ok(());
        }

        // Recording access does not mean modifying the pin count.
        if guard.pinned.iter().any(|(&x, _)| x == pid) {
            return Ok(());
        }

        Err(FrameNotFound)
    }

    fn add(&self, pid: PageId) {
        let mut guard = self.inner.lock().expect("Lock was somehow poisoned");

        assert!(guard.pinned.insert(pid, 0).is_none());
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

        match guard.pinned.remove(&pid) {
            Some(_) => Ok(()),
            None => Err(FrameNotFound),
        }
    }

    fn size(&self) -> usize {
        let guard = self.inner.lock().expect("Lock was somehow poisoned");

        guard.queue.len() + guard.pinned.len()
    }
}
