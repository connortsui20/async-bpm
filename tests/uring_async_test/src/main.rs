use async_disk_test::io_uring_async::IoUringAsync;
use io_uring::opcode;
use io_uring::types::Fd;
use send_wrapper::SendWrapper;
use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::thread;
use std::time::Instant;

/// Maximum number of `io_uring` entries (on my machine)
const IO_URING_ENTRIES: u16 = 1 << 15;

async fn test_operation_with_id(uring: Rc<IoUringAsync>, fd: RawFd, id: u64) {
    let mut buf = vec![0u8; 16];
    let buf_ptr = buf.as_mut_ptr();

    let entry = opcode::Read::new(Fd(fd), buf_ptr, 16).build();
    let entry = entry.user_data(id);

    let cqe = uring.push(entry).await;
    assert!(cqe.result() >= 0, "Operation error: {}", cqe.result());
}

fn spawn_runtime(fd: RawFd, operations: usize, uring: Rc<IoUringAsync>) {
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
                // Spawn a task that waits for the io_uring to become readable and handles completion
                // queue entries accordingly.
                tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));

                for i in 0..operations as u64 {
                    let id = i % IO_URING_ENTRIES as u64;
                    tokio::task::spawn_local(test_operation_with_id(uring.clone(), fd, id))
                        .await
                        .unwrap();
                }
            })
            .await;
    });
}

fn sequential_test(fd: RawFd, operations: usize) {
    let now = Instant::now();

    {
        let uring = IoUringAsync::new(IO_URING_ENTRIES as u32).unwrap();
        spawn_runtime(fd, operations, Rc::new(uring));
    }

    let elapsed = now.elapsed();
    println!(
        "{} sequential read operations took: {:.2?}",
        operations, elapsed
    );
}

fn parallel_test(fd: RawFd, threads: usize, operations: usize) {
    let now = Instant::now();

    let uring = IoUringAsync::new(IO_URING_ENTRIES as u32).unwrap();
    let lifecycles = uring.lifecycles();

    thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                let local_uring =
                    IoUringAsync::new_from_lifecycles(IO_URING_ENTRIES as u32, lifecycles.clone())
                        .unwrap();
                spawn_runtime(fd, operations / threads, Rc::new(local_uring));
            });
        }
    });

    let elapsed = now.elapsed();
    println!(
        "{} parallel read operations took: {:.2?}",
        operations, elapsed
    );
}

fn main() {
    let file = File::open("db.test").unwrap();
    let raw_fd = file.as_raw_fd();

    let threads = 8;
    for operations in [1 << 16, 1 << 18, 1 << 20, 1 << 22, 1 << 24] {
        parallel_test(raw_fd, threads, operations);
    }

    // for operations in [1 << 16, 1 << 18, 1 << 20] {
    //     sequential_test(raw_fd, operations);
    // }
}
