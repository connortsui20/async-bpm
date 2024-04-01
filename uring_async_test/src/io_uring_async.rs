use chashmap::CHashMap;
use io_uring::{squeue::Entry as SqEntry, IoUring};
use std::ops::DerefMut;
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::unix::AsyncFd;

use crate::op_futures::{Lifecycle, Op, OpInner};

/// A single-threaded instance of `io_uring`
pub struct IoUringAsync {
    uring: Rc<IoUring>, // Ensure `!Send`
    // TODO make this a dedicated type to create IoUringAsyncs out of
    lifecycles: Arc<CHashMap<u64, Lifecycle>>,
}

impl AsRawFd for IoUringAsync {
    fn as_raw_fd(&self) -> RawFd {
        self.uring.as_raw_fd()
    }
}

impl IoUringAsync {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        Ok(Self {
            uring: Rc::new(io_uring::IoUring::new(entries)?),
            lifecycles: Arc::new(CHashMap::with_capacity(4096)),
        })
    }

    // TODO figure out a better way to share between threads
    pub fn lifecycles(&self) -> Arc<CHashMap<u64, Lifecycle>> {
        self.lifecycles.clone()
    }

    pub fn new_from_lifecycles(
        entries: u32,
        lifecycles: Arc<CHashMap<u64, Lifecycle>>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            uring: Rc::new(io_uring::IoUring::new(entries)?),
            lifecycles,
        })
    }

    pub async fn listen(uring: Rc<IoUringAsync>) {
        let async_fd = AsyncFd::new(uring).unwrap();

        loop {
            let mut guard = async_fd.readable().await.unwrap();
            guard.get_inner().handle_cqe();
            guard.clear_ready();
        }
    }

    // It is on the caller to provide a unique ID
    pub fn push(&self, entry: SqEntry) -> Op {
        let id = entry.get_user_data();

        // First, check if someone else has already requested this specific ID
        if self.lifecycles.contains_key(&id) {
            return Op {
                inner: Some(OpInner {
                    lifecycles: self.lifecycles.clone(),
                    id,
                }),
            };
        }

        let res = self.lifecycles.insert(id, Lifecycle::Unsubmitted);
        debug_assert!(res.is_none());

        // Safety: Since `IoUringAsync` is not `Send`, and since cloning an `IoUringAsync` does not
        // actually clone the inner `IoUring` instance, only 1 `SubmissionQueue` can exist in the
        // context of this `IoUring` instance, so getting the submission queue with
        // `submission_shared` is safe.
        let mut submission_queue = unsafe { self.uring.submission_shared() };

        // Safety: The call to `push` is safe because this function returns an `Op` that will not
        // get dropped until it polls a `Lifecycle::Completed`, at which the operation has finished
        // and it is safe to drop.
        // Note that unless the submission queue is full, this should only have 1 iteration
        while unsafe { submission_queue.push(&entry).is_err() } {
            self.submit()
                .expect("Something went wrong with `io_uring_enter");
        }

        Op {
            inner: Some(OpInner {
                lifecycles: self.lifecycles.clone(),
                id,
            }),
        }
    }

    pub fn handle_cqe(&self) {
        // let mut guard = self.lifecycles.borrow_mut();

        // Safety: Since `IoUringAsync` is not `Send`, and since cloning an `IoUringAsync` does not
        // actually clone the inner `IoUring` instance, only 1 `SubmissionQueue` can exist in the
        // context of this `IoUring`, so getting the submission queue with `submission_shared` is
        // safe.
        let completion_queue = unsafe { self.uring.completion_shared() };

        for cqe in completion_queue {
            // Find our local record of this operation
            let id = cqe.user_data();
            let mut guard = self
                .lifecycles
                .get_mut(&id)
                .expect("Completion event does not match any submission ID");
            let lifecycle = guard.deref_mut();

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

    /// Submit all queued submission queue events to the kernel. Internally calls `io_uring_enter`.
    pub fn submit(&self) -> std::io::Result<usize> {
        self.uring.submit()
    }
}
