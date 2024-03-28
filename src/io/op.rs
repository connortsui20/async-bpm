//! Implementation of futures for `io_uring` operations.

use io_uring::cqueue::Entry as CqEntry;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;

/// The `IoUring` lifecycle state.
pub(super) enum Lifecycle {
    /// The operation has been entered onto the submission queue, but has not been submitted to the
    /// kernel via `io_uring_enter` by the asynchronous runtime.
    Unsubmitted,
    /// The operation has been submitted to the kernel and we are waiting for it to finish.
    Waiting(std::task::Waker),
    /// The kernel has finished the operation and has returned a completion queue entry.
    Completed(CqEntry),
}

#[derive(Clone)]
pub(super) struct OpInner {
    /// A thread-local table of unique operation IDs mapped to current in-flight operation states.
    pub(super) operations: Rc<RefCell<HashMap<u64, Lifecycle>>>,
    /// A unique ID to an `io_uring` operation
    pub(super) id: u64,
}

/// A wrapper around an optionally owned `OpInner` type.
pub struct Op {
    /// Ownership over the `OpInner` value is moved to a new tokio task when an `Op` is dropped.
    pub(super) inner: Option<OpInner>,
}

impl Future for OpInner {
    type Output = CqEntry;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut guard = self.operations.borrow_mut();

        // This is safe to unwrap since we only remove the `Lifecycle` from the table after we get
        // dropped by our parent `Op`
        let lifecycle = guard.get_mut(&self.id).unwrap();

        // We are only ready once the lifecycle is in the `Completed` state
        match lifecycle {
            Lifecycle::Unsubmitted => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                std::task::Poll::Pending
            }
            Lifecycle::Waiting(_) => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                std::task::Poll::Pending
            }
            Lifecycle::Completed(cqe) => std::task::Poll::Ready(cqe.clone()),
        }
    }
}

impl Drop for OpInner {
    fn drop(&mut self) {
        let mut guard = self.operations.borrow_mut();
        let lifecycle = guard.remove(&self.id);

        let Some(Lifecycle::Completed(_)) = &lifecycle else {
            unreachable!("`OpInner` was dropped before completing its operation");
        };
    }
}

impl Future for Op {
    type Output = CqEntry;

    /// Simply poll the inner `OpInner`.
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // `inner` is only `None` after we drop `Op`, so we can unwrap safely
        std::pin::Pin::new(self.inner.as_mut().unwrap()).poll(cx)
    }
}

/// If `Op` gets dropped before it has finished its operation, someone has to clean up.
/// The inner future is spawned again as a task onto the current thread, where it will complete.
impl Drop for Op {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();
        let guard = inner.operations.borrow();

        // This is safe to unwrap since we only remove the `Lifecycle` from the table after the
        // `OpInner` gets dropped
        match guard.get(&inner.id).unwrap() {
            Lifecycle::Completed(_) => {}
            _ => {
                drop(guard);
                tokio::task::spawn_local(inner);
            }
        }
    }
}