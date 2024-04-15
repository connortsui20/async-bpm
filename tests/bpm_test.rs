use async_bpm::{bpm::BufferPoolManager, page::PageId};
use send_wrapper::SendWrapper;
use std::thread;
use std::{ops::DerefMut, rc::Rc, sync::Arc};
use tokio::{runtime::Builder, task::LocalSet};

#[test]
fn test_new_disk_manager() {
    const THREADS: usize = 95;

    let bpm = Arc::new(BufferPoolManager::new(4, THREADS));

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
                    let ph = match bpm_clone.create_page(&pid).await {
                        None => bpm_clone.get_page(&pid).await.unwrap(),
                        Some(ph) => ph,
                    };

                    let mut guard = ph.write().await;
                    guard.deref_mut().fill(b' ' + i as u8);

                    drop(guard);

                    ph.evict().await;
                });

                rt.block_on(local);

                loop {}
            });
        }
    });
}
