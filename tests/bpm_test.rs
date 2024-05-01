use async_bpm::{bpm::BufferPoolManager, page::PageId};
use rand::Rng;
use std::fs::File;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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

    const THREADS: usize = 32;

    BufferPoolManager::initialize(64, THREADS * 2);

    let bpm = BufferPoolManager::get();

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
                });

                rt.block_on(local);
            });
        }
    });
}

#[test]
fn test_simple() {
    const TASKS: usize = 2; // tasks per thread
    const ITERATIONS: usize = 1024; // iterations per task

    const FRAMES: usize = 64;
    const DISK_PAGES: usize = 256;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let log_file = File::create("simple.log").unwrap();

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

    BufferPoolManager::initialize(FRAMES, DISK_PAGES);
    let bpm = BufferPoolManager::get();

    let rt = bpm.build_thread_runtime();

    let local = LocalSet::new();

    for task in 0..TASKS {
        local.spawn_local(async move {
            let mut rng = rand::thread_rng();

            for iteration in 0..ITERATIONS {
                let id = rng.gen_range(0..DISK_PAGES) as u64;
                let pid = PageId::new(id);
                let ph = bpm.get_page(&pid).await;

                trace!("Start iteration {} {} ({})", task, iteration, pid);

                let guard = ph.read().await;
                let slice = guard.deref();
                std::hint::black_box(slice);
                drop(guard);

                COUNTER.fetch_add(1, Ordering::SeqCst);

                trace!("Finish iteration {} {} ({})", task, iteration, pid);
            }
        });
    }

    rt.block_on(local);

    assert_eq!(COUNTER.load(Ordering::SeqCst), TASKS * ITERATIONS);
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

    const THREADS: usize = 96;
    BufferPoolManager::initialize(128, THREADS * 2);

    let bpm = BufferPoolManager::get();

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
