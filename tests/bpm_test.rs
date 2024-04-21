use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::fs::File;
use std::sync::Mutex;
use std::thread;
use std::{ops::DerefMut, sync::Arc};
use tokio::task::LocalSet;
use tracing::{trace, Level};

#[test]
#[ignore]
fn test_bpm_threads() {
    trace!("Starting test_bpm_threads");

    let log_file = File::create("test_bpm_threads.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::WARN)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    const THREADS: usize = 96;

    let bpm = Arc::new(BufferPoolManager::new(2, 256));

    // Spawn the eviction thread / single task
    let bpm_evictor = bpm.clone();
    bpm_evictor.spawn_evictor();

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            let bpm_clone = bpm.clone();

            s.spawn(move || {
                let rt = bpm_clone.build_thread_runtime();

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let pid = PageId::new(i as u64);
                    let ph = bpm_clone.get_page(&pid).await;

                    let mut guard = ph.write().await;

                    guard.deref_mut().fill(b' ' + i as u8);
                    guard.flush().await;

                    drop(guard);

                    loop {}
                });

                rt.block_on(local);
            });
        }
    });
}
