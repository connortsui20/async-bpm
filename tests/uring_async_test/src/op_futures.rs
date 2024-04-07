use chashmap::CHashMap;
use io_uring::cqueue::Entry as CqEntry;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// The `IoUring` lifecycle state.
pub enum Lifecycle {
    /// The operation has been entered onto the submission queue, but has not been submitted to the
    /// kernel via `io_uring_enter` by the asynchronous runtime.
    Unsubmitted,
    /// The operation has been submitted to the kernel and we are waiting for it to finish.
    Waiting(std::task::Waker),
    /// The kernel has finished the operation and has returned a completion queue entry.
    Completed(CqEntry),
}

/// A wrapper around an optionally owned `OpInner` type.
pub struct Op {
    /// Ownership over the `OpInner` value is moved to a new tokio task when an `Op` is dropped.
    pub inner: Option<OpInner>,
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

impl Drop for Op {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();
        let guard = inner.lifecycles.get(&inner.id).unwrap();

        match guard.deref() {
            Lifecycle::Completed(_) => {}
            _ => {
                drop(guard);
                tokio::task::spawn_local(inner);
            }
        }
    }
}

// TODO make private
#[derive(Clone)]
pub struct OpInner {
    pub lifecycles: Arc<CHashMap<u64, Lifecycle>>,
    pub id: u64,
}

impl Future for OpInner {
    type Output = CqEntry;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut guard = self.lifecycles.get_mut(&self.id).unwrap();
        let lifecycle = guard.deref_mut();

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
        let Some(Lifecycle::Completed(_)) = self.lifecycles.remove(&self.id) else {
            unreachable!("`OpInner` was dropped before completing its operation");
        };
    }
}
