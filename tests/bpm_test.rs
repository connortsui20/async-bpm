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
    let log_file = File::create("bpm_test.log").unwrap();

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

    const THREADS: usize = 4;

    let bpm = Arc::new(BufferPoolManager::new(2, THREADS));

    thread::scope(|s| {
        // Spawn the eviction thread / single task
        s.spawn(|| {
            let bpm_clone = bpm.clone();
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
                bpm_clone.evictor().await;
            });

            rt.block_on(local);

            loop {}
        });

        // Spawn all threads
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
