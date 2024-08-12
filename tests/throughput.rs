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
use tokio::task::JoinSet;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

#[test]
fn throughput() {
    const THREADS: usize = 8;
    const TASKS: usize = 32; // tasks per thread
    const OPERATIONS: usize = 1 << 20;

    const THREAD_OPERATIONS: usize = OPERATIONS / THREADS;
    const ITERATIONS: usize = THREAD_OPERATIONS / TASKS; // iterations per task

    const FRAMES: usize = GIGABYTE_PAGES;
    const STORAGE_PAGES: usize = 32 * GIGABYTE_PAGES;

    BufferPoolManager::initialize(FRAMES, STORAGE_PAGES);
    let bpm = BufferPoolManager::get();

    let coin = Bernoulli::new(20.0 / 100.0).unwrap();

    println!("Operations: {OPERATIONS}");

    // Spawn all threads
    thread::scope(|s| {
        for _thread in 0..THREADS {
            s.spawn(move || {
                BufferPoolManager::start_thread(async move {
                    let mut set = JoinSet::new();
                    for _task in 0..TASKS {
                        let handle = BufferPoolManager::spawn_local(async move {
                            let mut rng = rand::thread_rng();

                            for _iteration in 0..ITERATIONS {
                                let id = rng.gen_range(0..STORAGE_PAGES) as u64;
                                let pid = PageId::new(id);
                                let ph = bpm.get_page(&pid).await.unwrap();

                                if coin.sample(&mut rng) {
                                    let mut guard = ph.write().await;
                                    guard.deref_mut().fill(b'a');
                                    guard.flush().await.unwrap();
                                } else {
                                    let guard = ph.read().await;
                                    let slice = guard.deref();
                                    std::hint::black_box(slice);
                                }

                                COUNTER.fetch_add(1, Ordering::SeqCst);
                            }
                        });

                        set.spawn(handle);
                    }

                    // Execute all tasks concurrently.
                    while let Some(res) = set.join_next().await {
                        let inner_res = res.unwrap();
                        inner_res.unwrap();
                    }
                });
            });
        }

        s.spawn(|| {
            let duration = std::time::Duration::from_secs(1);
            while COUNTER.load(Ordering::SeqCst) < THREADS * TASKS * ITERATIONS {
                println!("Counter is at: {:?}", COUNTER);
                std::thread::sleep(duration);
            }

            std::process::exit(0);
        });
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), THREADS * TASKS * ITERATIONS);
}
