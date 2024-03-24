use super::op::Lifecycle;
use io_uring::IoUring;
use slab::Slab;
use std::{
    cell::RefCell,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
};
use tokio::io::unix::AsyncFd;

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

// impl IoUringAsync {
//     pub async fn listen(uring: Rc<IoUringAsync>) {
//         let async_fd = AsyncFd::new(uring).unwrap();
//         loop {
//             let mut guard = async_fd.readable().await.unwrap();
//             guard.get_inner().handle_cqe();
//             guard.clear_ready();
//         }
//     }

//     pub fn generic_new(entries: u32) -> std::io::Result<Self> {
//         Ok(Self {
//             uring: Rc::new(io_uring::IoUring::generic_new(entries)?),
//             slab: Rc::new(RefCell::new(slab::Slab::new())),
//         })
//     }

//     pub fn push(&self, entry: impl Into<S>) -> Op<C> {
//         let mut guard = self.slab.borrow_mut();
//         let index = guard.insert(Lifecycle::Submitted);
//         let entry = entry.into().user_data(index.try_into().unwrap());
//         while unsafe { self.uring.submission_shared().push(&entry).is_err() } {
//             self.uring.submit().unwrap();
//         }
//         Op {
//             inner: Some(OpInner {
//                 slab: self.slab.clone(),
//                 index: index,
//             }),
//         }
//     }

//     pub fn handle_cqe(&self) {
//         let mut guard = self.slab.borrow_mut();
//         while let Some(cqe) = unsafe { self.uring.completion_shared() }.next() {
//             let index = cqe.user_data();
//             let lifecycle = &mut guard[index.try_into().unwrap()];
//             match lifecycle {
//                 Lifecycle::Submitted => {
//                     *lifecycle = Lifecycle::Completed(cqe);
//                 }
//                 Lifecycle::Waiting(waker) => {
//                     waker.wake_by_ref();
//                     *lifecycle = Lifecycle::Completed(cqe);
//                 }
//                 Lifecycle::Completed(cqe) => {
//                     println!(
//                         "multishot operations not implemented: {}, {}",
//                         cqe.user_data(),
//                         cqe.result()
//                     );
//                 }
//             }
//         }
//     }

//     /// Submit all queued submission queue events to the kernel.
//     pub fn submit(&self) -> std::io::Result<usize> {
//         self.uring.submit()
//     }
// }
