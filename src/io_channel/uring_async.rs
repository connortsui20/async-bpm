use super::op::{Lifecycle, Op, OpInner};
use io_uring::{squeue::Entry as SqEntry, IoUring};
use slab::Slab;
use std::{
    cell::RefCell,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
};
use tokio::io::unix::AsyncFd;

/// A thread-local `io_uring` instance
#[derive(Clone)]
pub struct IoUringAsync {
    uring: Rc<IoUring>,
    slab: Rc<RefCell<Slab<Lifecycle>>>,
}

impl AsRawFd for IoUringAsync {
    fn as_raw_fd(&self) -> RawFd {
        self.uring.as_raw_fd()
    }
}

impl IoUringAsync {
    pub fn new(entries: u16) -> std::io::Result<Self> {
        Ok(Self {
            uring: Rc::new(io_uring::IoUring::new(entries as u32)?),
            slab: Rc::new(RefCell::new(Slab::with_capacity((entries * 2) as usize))),
        })
    }

    pub async fn listen(&self) {
        let async_fd = AsyncFd::new(self.clone()).unwrap();

        loop {
            let mut guard = async_fd.readable().await.unwrap();
            guard.get_inner().handle_cqe();
            guard.clear_ready();
        }
    }

    /// Pushes an entry onto the submission queue.
    ///
    /// The caller must ensure that the entry has a unique ID as its user data,
    /// otherwise this function will panic.
    pub fn push(&self, entry: SqEntry) -> Op {
        let id = entry.get_user_data();

        let mut guard = self.slab.borrow_mut();

        todo!()

        // let index = guard.insert(Lifecycle::Unsubmitted);
        // let entry = entry.into().user_data(index.try_into().unwrap());
        // while unsafe { self.uring.submission_shared().push(&entry).is_err() } {
        //     self.uring.submit().unwrap();
        // }

        // Op {
        //     inner: Some(OpInner {
        //         slab: self.slab.clone(),
        //         index: index,
        //     }),
        // }
    }

    pub fn handle_cqe(&self) {
        let mut guard = self.slab.borrow_mut();
        while let Some(cqe) = unsafe { self.uring.completion_shared() }.next() {
            let index = cqe.user_data();
            let lifecycle = &mut guard[index.try_into().unwrap()];
            match lifecycle {
                Lifecycle::Unsubmitted => {
                    *lifecycle = Lifecycle::Completed(cqe);
                }
                Lifecycle::Waiting(waker) => {
                    waker.wake_by_ref();
                    *lifecycle = Lifecycle::Completed(cqe);
                }
                Lifecycle::Completed(cqe) => {
                    println!(
                        "multishot operations not implemented: {}, {}",
                        cqe.user_data(),
                        cqe.result()
                    );
                }
            }
        }
    }

    /// Submit all queued submission queue events to the kernel.
    pub fn submit(&self) -> std::io::Result<usize> {
        self.uring.submit()
    }
}
