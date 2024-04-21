use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::fs::File;
use std::sync::Mutex;
use std::thread;
use std::{ops::DerefMut, sync::Arc};
use tokio::sync::Barrier;
use tokio::task::LocalSet;
use tracing::{info, trace, Level};

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

    let bpm = Arc::new(BufferPoolManager::new(2, THREADS * 2));

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

                    #[allow(clippy::empty_loop)]
                    loop {}
                });

                rt.block_on(local);
            });
        }
    });
}

#[test]
#[ignore]
fn test_bpm_upwards() {
    trace!("Starting test_bpm_upwards");

    let log_file = File::create("test_bpm_upwards.log").unwrap();

    let stdout_subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .without_time()
        .with_max_level(Level::INFO)
        .with_writer(Mutex::new(log_file))
        .finish();
    tracing::subscriber::set_global_default(stdout_subscriber).unwrap();

    const THREADS: usize = 95;

    let bpm = Arc::new(BufferPoolManager::new(128, THREADS * 2));

    // Spawn the eviction thread / single task
    let bpm_evictor = bpm.clone();
    bpm_evictor.spawn_evictor();

    // Spawn all threads
    thread::scope(|s| {
        let b = Arc::new(Barrier::new(THREADS));

        for i in 0..THREADS {
            let bpm_clone = bpm.clone();
            let barrier = b.clone();

            s.spawn(move || {
                let rt = bpm_clone.build_thread_runtime();

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let pid1 = PageId::new(i as u64);
                    let ph1 = bpm_clone.get_page(&pid1).await;

                    let mut write_guard = ph1.write().await;
                    write_guard.deref_mut().fill(b' ' + i as u8);
                    write_guard.flush().await;

                    let pid2 = PageId::new((i + 1) as u64);
                    let ph2 = bpm_clone.get_page(&pid2).await;

                    // Check if the next thread has finished
                    loop {
                        let read_guard = ph2.read().await;
                        let val = read_guard[0];
                        drop(read_guard);

                        if i == THREADS - 1 || val == b' ' + (i as u8) + 1 {
                            trace!("Checked {} to be correct", pid2);
                            break;
                        }
                        trace!("{} is not yet correct", pid2);
                    }

                    drop(write_guard);

                    barrier.wait().await;

                    info!("Finished test_bpm_threads test!");

                    #[allow(clippy::empty_loop)]
                    loop {}
                });

                rt.block_on(local);
            });
        }
    });
}
