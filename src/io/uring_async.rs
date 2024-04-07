use super::op::{Lifecycle, Op, OpInner};
use derivative::Derivative;
use io_uring::{squeue::Entry as SqEntry, IoUring};
use std::{
    cell::RefCell,
    collections::HashMap,
    io,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
};
use tokio::io::unix::AsyncFd;

/// The default number of `io_uring` submission entries.
pub const IO_URING_DEFAULT_ENTRIES: u16 = 1 << 12; // 4096

/// A thread-local `io_uring` instance that can be embedded in an asynchronous runtime.
///
/// Implicitly, `IoUringAsync` _must_ be thread-local since it is `!Send`.
#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub struct IoUringAsync {
    /// The thread-local `io_uring` instance.
    #[derivative(Debug = "ignore")]
    pub(crate) uring: Rc<RefCell<IoUring>>,

    /// A thread-local table of unique operation IDs mapped to current in-flight operation states.
    operations: Rc<RefCell<HashMap<u64, Lifecycle>>>,
}

impl IoUringAsync {
    /// Creates a new thread-local `IoUringAsync` instance that can support holding `entries`
    /// submission queue entries.
    pub fn new(entries: u16) -> io::Result<Self> {
        Ok(Self {
            uring: Rc::new(RefCell::new(io_uring::IoUring::new(entries as u32)?)),
            operations: Rc::new(RefCell::new(HashMap::with_capacity(entries as usize))),
        })
    }

    /// Calls [`IoUringAsync::new`] with `IO_URING_DEFAULT_ENTRIES` entries.
    pub fn try_default() -> io::Result<Self> {
        Self::new(IO_URING_DEFAULT_ENTRIES)
    }

    /// Continuously polls the completion queue and updates any local in-flight operation states.
    ///
    /// This `Future` _must_ be placed onto the task queue of a thread _at least_ once, otherwise no
    /// `Op` futures will ever make progress.
    pub async fn listener(self: &Rc<Self>) -> ! {
        let async_fd = AsyncFd::new(self.clone()).unwrap();

        loop {
            let mut guard = async_fd.writable().await.unwrap();

            guard.get_inner().poll();
            guard.clear_ready();
        }
    }

    /// Continuously submits entries on the submission queue.
    ///
    /// Note that submission is not the same thing as pushing an operation onto the `io_uring`
    /// submission queue. Use [`IoUringAsync::push`] to place operations onto the submission queue,
    /// and use [`IoUringAsync::submit`] to manually submit said operations to the kernel.
    ///
    /// Will panic if submission fails.
    ///
    /// Either this `Future` _must_ be placed onto the task queue of a thread _at least_ once, or
    /// the caller must ensure that they manually call [`IoUringAsync::submit`] at regular intervals
    /// otherwise no `Op` futures will ever make progress.
    pub async fn submitter(self: &Rc<Self>) -> ! {
        let async_fd = AsyncFd::new(self.clone()).unwrap();

        loop {
            let mut guard = async_fd.writable().await.unwrap();

            guard.get_inner().submit().expect(
                "Something went wrong when trying to submit \
                `io_uring` operation events on the submission queue",
            );
            guard.clear_ready();
        }
    }

    /// Submit all queued submission queue events to the kernel.
    pub fn submit(&self) -> std::io::Result<usize> {
        self.uring.borrow().submit()
    }

    /// Pushes an entry onto the submission queue.
    ///
    /// The caller must ensure that the entry has a unique 64-bit integer ID as its user data,
    /// otherwise this function will panic.
    ///
    /// # Safety
    ///
    /// Developers must ensure that parameters of the entry (such as a registered buffer) are valid
    /// and will be valid for the entire duration of the operation, otherwise it will cause
    /// undefined behavior
    ///
    /// This safety contract is almost identical to the contract for
    /// [`SubmissionQueue::push`](io_uring::SubmissionQueue::push).
    pub unsafe fn push(&self, entry: SqEntry) -> Op {
        let id = entry.get_user_data();

        let mut operations_guard = self.operations.borrow_mut();

        let index = operations_guard.insert(id, Lifecycle::Unsubmitted);
        assert!(
            index.is_none(),
            "Tried to start an IO event with id {id} that was already in progress, \
            with current state {:?}",
            index.unwrap()
        );

        let mut uring_guard = self.uring.borrow_mut();
        let mut submission_queue = uring_guard.submission();

        // Safety: We must ensure that the parameters of this entry are valid for the entire
        // duration of the operation, and this is guaranteed by this function's safety contract.
        while unsafe { submission_queue.push(&entry).is_err() } {
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

    /// Poll the `io_uring` completion queue for completed events.
    ///
    /// This function will iterate over any completed `io_uring` operations and update the
    /// respective `Lifecycle` state in the `HashMap` of in-flight operations.
    ///
    /// It is then on the caller to `.await` the [`Future`](std::future::Future) returned by
    /// [`IoUringAsync::push`] to observe the result of the operation, as well as remove it from the
    /// `HashMap` of current in-flight operations by [`Future`](std::future::Future).
    pub fn poll(&self) {
        let mut uring_guard = self.uring.borrow_mut();
        let completion_queue = uring_guard.completion();

        let mut guard = self.operations.borrow_mut();

        // Iterate through all of the completed operations
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

/// Intended for use with the `tokio` runtime.
impl AsRawFd for IoUringAsync {
    fn as_raw_fd(&self) -> RawFd {
        self.uring.borrow().as_raw_fd()
    }
}
