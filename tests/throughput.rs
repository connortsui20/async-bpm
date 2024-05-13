use async_bpm::{
    bpm::BufferPoolManager,
    page::{PageId, PAGE_SIZE},
};
use rand::distributions::Bernoulli;
use rand::{distributions::Distribution, Rng};
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};
use tokio::task::LocalSet;
use tracing::trace;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

#[test]
fn bench() {
    const THREADS: usize = 16;
    const TASKS: usize = 32; // tasks per thread
    const OPERATIONS: usize = 1 << 20;

    const THREAD_OPERATIONS: usize = OPERATIONS / THREADS;
    const ITERATIONS: usize = THREAD_OPERATIONS / TASKS; // iterations per task

    const FRAMES: usize = GIGABYTE_PAGES;
    const DRIVE_PAGES: usize = 32 * GIGABYTE_PAGES;

    BufferPoolManager::initialize(FRAMES, DRIVE_PAGES);
    let bpm = BufferPoolManager::get();

    let coin = Bernoulli::new(20.0 / 100.0).unwrap();

    println!("Operations: {OPERATIONS}");

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
                                let id = rng.gen_range(0..DRIVE_PAGES) as u64;
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

        s.spawn(|| {
            let duration = std::time::Duration::from_secs(1);
            while COUNTER.load(Ordering::SeqCst) < THREADS * TASKS * ITERATIONS {
                println!("Counter is at: {:?}", COUNTER);
                std::thread::sleep(duration);
            }
        });
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), THREADS * TASKS * ITERATIONS);
}
