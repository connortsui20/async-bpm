use async_bpm::{bpm::BufferPoolManager, page::PageId};
use send_wrapper::SendWrapper;
use std::fs::File;
use std::sync::Mutex;
use std::thread;
use std::{ops::DerefMut, rc::Rc, sync::Arc};
use tokio::{runtime::Builder, task::LocalSet};
use tracing::Level;

#[test]
#[ignore]
fn test_bpm_threads() {
    let log_file = File::create("test_bpm_threads.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::TRACE)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    const THREADS: usize = 8;

    let bpm = Arc::new(BufferPoolManager::new(2, THREADS));

    // Spawn the eviction thread / single task
    let bpm_evictor = bpm.clone();
    thread::spawn(move || {
        let dmh = bpm_evictor.get_disk_manager();
        let uring = Rc::new(dmh.get_uring());

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

        let local = LocalSet::new();
        local.spawn_local(async move {
            bpm_evictor.evictor().await;
        });

        rt.block_on(local);

        loop {}
    });

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            let bpm_clone = bpm.clone();

            s.spawn(move || {
                let dmh = bpm_clone.get_disk_manager();
                let uring = Rc::new(dmh.get_uring());

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

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let pid = PageId::new(i as u64);
                    let ph = bpm_clone.get_page(&pid).await;

                    let mut guard = ph.write().await;

                    guard.deref_mut().fill(b' ' + i as u8);

                    drop(guard);

                    loop {}
                });

                rt.block_on(local);
            });
        }
        // run eviction thread
    });
}

#[test]
#[ignore]
fn test_bpm_no_eviction() {
    let log_file = File::create("test_bpm_no_eviction.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::DEBUG)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    const THREADS: usize = 95;

    let bpm = Arc::new(BufferPoolManager::new(128, THREADS));

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            let bpm_clone = bpm.clone();

            s.spawn(move || {
                let dmh = bpm_clone.get_disk_manager();
                let uring = Rc::new(dmh.get_uring());

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

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let pid = PageId::new(i as u64);
                    let ph = bpm_clone.get_page(&pid).await;

                    let mut guard = ph.write().await;

                    guard.deref_mut().fill(b' ' + i as u8);
                    guard.flush().await;

                    drop(guard);

                    loop {
                        tokio::task::yield_now().await;
                    }
                });

                // // Check that everyone else has finished
                // local.spawn_local(async move {
                //     for j in 0..THREADS {

                //     }
                // });

                rt.block_on(local);
            });
        }
        // run eviction thread
    });
}
