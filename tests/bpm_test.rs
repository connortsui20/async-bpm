use async_bpm::{bpm::BufferPoolManager, page::PageId};
use send_wrapper::SendWrapper;
use std::thread;
use std::{ops::DerefMut, rc::Rc, sync::Arc};
use tokio::{runtime::Builder, task::LocalSet};

#[test]
#[ignore]
fn test_bpm_threads() {
    const THREADS: usize = 95;

    let bpm = Arc::new(BufferPoolManager::new(4, THREADS));

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
        });

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
                    let ph = match bpm_clone.create_page(&pid).await {
                        None => bpm_clone.get_page(&pid).await.unwrap(),
                        Some(ph) => ph,
                    };

                    let mut guard = ph.write().await;

                    guard.deref_mut().fill(b' ' + i as u8);

                    drop(guard);

                    loop {
                        tokio::task::yield_now().await;
                    }
                });

                rt.block_on(local);
            });
        }
        // run eviction thread
    });
}
