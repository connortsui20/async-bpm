use async_bpm::{bpm::BufferPoolManager, bpm::BPM, page::PageId};
use std::fs::File;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tokio::sync::Barrier;
use tokio::task::LocalSet;
use tracing::debug;
use tracing::{info, trace, Level};

#[test]
#[ignore]
fn test_bpm_threads() {
    std::fs::remove_file("db.test").unwrap();

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

    const THREADS: usize = 4;

    BufferPoolManager::initialize(16, THREADS * 4);

    let bpm = BPM.get().unwrap();

    debug!("Testing test_bpm_threads");

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            s.spawn(move || {
                let rt = bpm.build_thread_runtime();

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let index = 2 * i as u8;
                    let pid = PageId::new(index as u64);
                    let ph = bpm.get_page(&pid).await;

                    {
                        let mut guard = ph.write().await;
                        guard.deref_mut().fill(b' ' + index);
                        guard.flush().await;
                    }

                    loop {
                        tokio::task::yield_now().await;
                    }
                });

                local.spawn_local(async move {
                    let index = ((2 * i) + 1) as u8;
                    let pid = PageId::new(index as u64);
                    let ph = bpm.get_page(&pid).await;

                    {
                        let mut guard = ph.write().await;
                        guard.deref_mut().fill(b' ' + index);
                        guard.flush().await;
                    }

                    loop {
                        tokio::task::yield_now().await;
                    }
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

    BufferPoolManager::initialize(2, THREADS * 2);

    let bpm = BPM.get().unwrap();

    // Spawn all threads
    thread::scope(|s| {
        let b = Arc::new(Barrier::new(THREADS));

        for i in 0..THREADS {
            let barrier = b.clone();

            s.spawn(move || {
                let rt = bpm.build_thread_runtime();

                let local = LocalSet::new();
                local.spawn_local(async move {
                    let pid1 = PageId::new(i as u64);
                    let ph1 = bpm.get_page(&pid1).await;

                    let mut write_guard = ph1.write().await;
                    write_guard.deref_mut().fill(b' ' + i as u8);
                    write_guard.flush().await;

                    let pid2 = PageId::new((i + 1) as u64);
                    let ph2 = bpm.get_page(&pid2).await;

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
