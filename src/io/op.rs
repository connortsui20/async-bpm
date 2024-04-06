//! Implementation of futures for `io_uring` operations.

use io_uring::cqueue::Entry as CqEntry;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// The `IoUring` lifecycle state.
#[derive(Debug)]
pub(super) enum Lifecycle {
    /// The operation has been entered onto the submission queue, but has not been submitted to the
    /// kernel via `io_uring_enter` by the asynchronous runtime.
    Unsubmitted,
    /// The operation has been submitted to the kernel and we are waiting for it to finish.
    Waiting(std::task::Waker),
    /// The kernel has finished the operation and has returned a completion queue entry.
    Completed(CqEntry),
}

/// The inner representation of an `io_uring` operation.
///
/// Holds a reference to the thread-local `HashMap` of all in-flight operations, as well as a unique
/// operation ID to help uniquely identify the operation.
#[derive(Debug, Clone)]
pub(super) struct OpInner {
    /// A thread-local table of unique operation IDs mapped to current in-flight operation states.
    pub(super) operations: Rc<RefCell<HashMap<u64, Lifecycle>>>,
    /// A unique ID to an `io_uring` operation
    pub(super) id: u64,
}

impl Drop for OpInner {
    /// The `OpInner` type can only be dropped once the operation has reached the
    /// [`Completed`](Lifecycle::Completed) state, at which point it is safe to drop.
    fn drop(&mut self) {
        let mut guard = self.operations.borrow_mut();
        let lifecycle = guard.remove(&self.id);

        let Some(Lifecycle::Completed(_)) = &lifecycle else {
            unreachable!("`OpInner` was dropped before completing its operation");
        };
    }
}

impl Future for OpInner {
    type Output = CqEntry;

    /// We want to check if the operation has completed, at which point we will want to return
    /// [`Poll::Ready`].
    ///
    /// Otherwise, we will wait to be waken up by
    /// [`IoUringAsync::poll`](crate::io::IoUringAsync::poll).
    ///
    /// It is the caller's responsibility to ensure that there is a task or local daemon that is
    /// dedicated to polling `io_uring` via [`IoUringAsync::poll`](crate::io::IoUringAsync::poll).
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.operations.borrow_mut();

        // This is safe to unwrap since we only remove the `Lifecycle` from the table after we get
        // dropped by our parent `Op`
        let lifecycle = guard.get_mut(&self.id).unwrap();

        // We are only ready once the lifecycle is in the `Completed` state
        match lifecycle {
            Lifecycle::Unsubmitted => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Waiting(_) => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Completed(cqe) => Poll::Ready(cqe.clone()),
        }
    }
}

/// A wrapper around an optionally owned `OpInner` type.
#[derive(Debug)]
pub struct Op {
    /// Ownership over the `OpInner` value is moved to a new tokio task when an `Op` is dropped.
    pub(super) inner: Option<OpInner>,
}

impl Drop for Op {
    /// If `Op` gets dropped before it has finished its operation, someone has to clean up.
    // The inner future is spawned again as a task onto the current thread, where it will complete.
    fn drop(&mut self) {
        // We only take the `OpInner` out once (here during `drop`), so this is safe to unwrap
        let inner = self.inner.take().unwrap();
        let guard = inner.operations.borrow();

        // This is safe to unwrap since we only remove the `Lifecycle` from the table after the
        // `OpInner` gets dropped, and that _must_ happen after this gets dropped.
        match guard.get(&inner.id).unwrap() {
            Lifecycle::Completed(_) => {}
            _ => {
                drop(guard);
                tokio::task::spawn_local(inner);
            }
        }
    }
}

impl Future for Op {
    type Output = CqEntry;

    /// Simply polls the inner `OpInner`.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `inner` is only `None` after we drop `Op`, so we can unwrap safely
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll(cx)
    }
}
