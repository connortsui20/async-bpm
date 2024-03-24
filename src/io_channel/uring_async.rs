use super::op::{Lifecycle, Op, OpInner};
use io_uring::{squeue::Entry as SqEntry, IoUring};
use std::{
    cell::RefCell,
    collections::HashMap,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
};
use tokio::io::unix::AsyncFd;

/// A thread-local `io_uring` instance
#[derive(Clone)]
pub struct IoUringAsync {
    uring: Rc<IoUring>,
    /// A thread-local table of unique operation IDs mapped to current in-flight operation states.
    operations: Rc<RefCell<HashMap<u64, Lifecycle>>>,
}

impl IoUringAsync {
    pub fn new(entries: u16) -> std::io::Result<Self> {
        Ok(Self {
            uring: Rc::new(io_uring::IoUring::new(entries as u32)?),
            operations: Rc::new(RefCell::new(HashMap::with_capacity(entries as usize))),
        })
    }

    pub async fn listen(&self) {
        let async_fd = AsyncFd::new(self.clone()).unwrap();

        loop {
            let mut guard = async_fd.readable().await.unwrap();
            guard.get_inner().poll();
            guard.clear_ready();
        }
    }

    /// Submit all queued submission queue events to the kernel.
    pub fn submit(&self) -> std::io::Result<usize> {
        self.uring.submit()
    }

    /// Pushes an entry onto the submission queue.
    ///
    /// The caller must ensure that the entry has a unique 64-bit integer ID as its user data,
    /// otherwise this function will panic.
    pub fn push(&self, entry: SqEntry) -> Op {
        let id = entry.get_user_data();

        let mut guard = self.operations.borrow_mut();

        assert!(
            !guard.contains_key(&id),
            "Tried to create an `io_uring` operation that has the same ID as a \
            currently in-flight operation"
        );

        let index = guard.insert(id, Lifecycle::Unsubmitted);

        while unsafe { self.uring.submission_shared().push(&entry).is_err() } {
            // Help make progress
            self.submit().unwrap();
        }

        Op {
            inner: Some(OpInner {
                operations: self.operations.clone(),
                id,
            }),
        }
    }

    pub fn poll(&self) {
        let mut guard = self.operations.borrow_mut();

        // Safety: The main reason this is safe is because `IoUringAsync` is thread local, and more
        // specifically, is `!Send`.
        // Since `IoUringAsync` is not `Send`, and since cloning an `IoUringAsync` does not actually
        // clone the inner `IoUring` instance, only 1 `CompletionQueue` can exist in the context of
        // this thread-local `IoUringAsync` instance, thus we satisfy the safety contract of
        // `completion_shared()`.
        let completion_queue = unsafe { self.uring.completion_shared() };

        // Pop off all of the completion queue data
        for cqe in completion_queue {
            let id = cqe.user_data();

            // This is safe to unwrap since we only remove the `Lifecycle` from the table after the
            // owning `Op` gets dropped. Since `Op` is only dropped after it has polled/observed a
            // `Lifecycle::Completed`, and we only set them to completed here, we can guarantee that
            // the operation state is still mapped in the table.
            let lifecycle = guard.get_mut(&id).unwrap();

            // Set operation status to completed
            match lifecycle {
                Lifecycle::Unsubmitted => {
                    *lifecycle = Lifecycle::Completed(cqe);
                }
                Lifecycle::Waiting(waker) => {
                    waker.wake_by_ref();
                    *lifecycle = Lifecycle::Completed(cqe);
                }
                Lifecycle::Completed(cqe) => {
                    unimplemented!(
                        "multi-shot operations not implemented yet: {}, {}",
                        cqe.user_data(),
                        cqe.result()
                    );
                }
            }
        }
    }
}

impl AsRawFd for IoUringAsync {
    fn as_raw_fd(&self) -> RawFd {
        self.uring.as_raw_fd()
    }
}
