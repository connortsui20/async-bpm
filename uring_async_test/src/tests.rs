use crate::io_uring_async::IoUringAsync;
use io_uring::opcode::Nop;
use send_wrapper::SendWrapper;
use std::rc::Rc;

#[test]
fn example1() {
    let uring = Rc::new(IoUringAsync::new(8).unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        tokio::task::LocalSet::new()
            .run_until(async {
                tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));

                let fut1 = uring.push(Nop::new().build());
                let fut2 = uring.push(Nop::new().build());

                uring.submit().unwrap();

                let cqe1 = fut1.await;
                let cqe2 = fut2.await;

                assert!(cqe1.result() >= 0, "nop error: {}", cqe1.result());
                assert!(cqe2.result() >= 0, "nop error: {}", cqe2.result());
            })
            .await;
    });
}

#[test]
fn example2() {
    let uring = IoUringAsync::new(8).unwrap();
    let uring = Rc::new(uring);

    // Create a new current_thread runtime that submits all outstanding submission queue
    // entries as soon as the executor goes idle.
    let uring_clone = SendWrapper::new(uring.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .on_thread_park(move || {
            uring_clone.submit().unwrap();
        })
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        tokio::task::LocalSet::new()
            .run_until(async {
                tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));

                let cqe = uring.push(Nop::new().build()).await;
                assert!(cqe.result() >= 0, "nop error: {}", cqe.result());
            })
            .await;
    });
}
