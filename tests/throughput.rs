use async_bpm::{
    bpm::BufferPoolManager,
    page::{PageId, PAGE_SIZE},
};
use rand::distributions::Bernoulli;
use rand::{distributions::Distribution, Rng};
use std::{
    fs::File,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};
use tokio::task::LocalSet;
use tracing::{trace, Level};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

#[test]
fn bench() {
    // const THREADS: usize = 32;
    // const TASKS: usize = 64; // tasks per thread
    // const ITERATIONS: usize = 64; // iterations per task

    // const FRAMES: usize = 2 * GIGABYTE_PAGES;
    // const DISK_PAGES: usize = 8 * GIGABYTE_PAGES;

    const THREADS: usize = 32;
    const TASKS: usize = 1; // tasks per thread
    const ITERATIONS: usize = 64; // iterations per task

    const FRAMES: usize = 128;
    const DISK_PAGES: usize = 1024;

    let log_file = File::create("bench.log").unwrap();

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

    let coin = Bernoulli::new(0.0).unwrap();

    // Spawn all threads
    thread::scope(|s| {
        for thread in 0..THREADS {
            s.spawn(move || {
                let rt = bpm.build_thread_runtime();

                let local = LocalSet::new();

                local.spawn_local(async move {
                    for task in 0..TASKS {
                        tokio::task::spawn_local(async move {
                            let mut rng = rand::thread_rng();

                            for iteration in 0..ITERATIONS {
                                let id = rng.gen_range(0..DISK_PAGES) as u64;
                                let pid = PageId::new(id);
                                let ph = bpm.get_page(&pid).await;

                                trace!(
                                    "Start  thread {}, task {}, iteration {} ({})",
                                    thread,
                                    task,
                                    iteration,
                                    pid
                                );

                                if coin.sample(&mut rng) {
                                    let mut guard = ph.write().await;
                                    guard.deref_mut().fill(b'a');
                                    guard.flush().await;
                                } else {
                                    let guard = ph.read().await;
                                    let slice = guard.deref();
                                    std::hint::black_box(slice);
                                }

                                COUNTER.fetch_add(1, Ordering::SeqCst);

                                trace!(
                                    "Finish thread {}, task {}, iteration {} ({})",
                                    thread,
                                    task,
                                    iteration,
                                    pid
                                );
                            }
                        });
                    }
                });

                rt.block_on(local);
            });
        }
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), THREADS * TASKS * ITERATIONS);
}
