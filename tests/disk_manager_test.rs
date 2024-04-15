use async_bpm::{
    bpm::BufferPoolManager,
    disk::frame::Frame,
    page::{PageId, PAGE_SIZE},
};
use send_wrapper::SendWrapper;
use std::{io::IoSliceMut, ops::DerefMut, rc::Rc, sync::Arc};
use tokio::{runtime::Builder, task::LocalSet};

#[test]
fn test_new_disk_manager() {
    let bpm = Arc::new(BufferPoolManager::new(4, 4));
    let dmh = bpm.get_disk_manager();
    let uring = Rc::new(dmh.get_uring());

    let pid0 = PageId::new(0);
    let pid1 = PageId::new(1);
    let pid2 = PageId::new(2);

    let buf = vec![0u8; PAGE_SIZE].leak();
    let io_slice = IoSliceMut::new(buf);
    let mut frame = Frame::new(io_slice);

    let local = LocalSet::new();
    local.spawn_local(async move {
        frame.deref_mut().fill(b'C');
        let mut frame = dmh.write_from(pid0, frame).await.unwrap();

        frame.deref_mut().fill(b'A');
        let mut frame = dmh.write_from(pid1, frame).await.unwrap();

        frame.deref_mut().fill(b'T');
        let _frame = dmh.write_from(pid2, frame).await.unwrap();
    });

    let uring_daemon = SendWrapper::new(uring.clone());
    let rt = Arc::new(
        Builder::new_current_thread()
            .on_thread_park(move || {
                uring_daemon
                    .submit()
                    .expect("Was unable to submit `io_uring` operations");
                uring_daemon.poll();
            })
            .enable_all()
            .build()
            .unwrap(),
    );

    rt.block_on(local);
}
