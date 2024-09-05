use async_bpm::{
    page::{PageId, PAGE_SIZE},
    BufferPoolManager,
};
use core_affinity::CoreId;
use rand::distributions::Bernoulli;
use rand::{distributions::Distribution, Rng};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};
use tokio::{
    sync::Barrier,
    task::{JoinHandle, JoinSet},
};
use zipf::ZipfDistribution;

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

const THREADS: usize = 8;
const TASKS: usize = 128; // tasks per thread
const OPERATIONS: usize = 1 << 24;

const THREAD_OPERATIONS: usize = OPERATIONS / THREADS;
const ITERATIONS: usize = THREAD_OPERATIONS / TASKS; // iterations per task

const FRAMES: usize = GIGABYTE_PAGES;
const STORAGE_PAGES: usize = 32 * GIGABYTE_PAGES;

/// The 30% was taken from the Redshift paper "Why TPC Is Not Enough", Table 2.
const WRITE_RATIO: f64 = 30.0 / 100.0;
const ZIPF_EXP: f64 = 1.1;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

// #[test]
// #[ignore]
// fn bench_random() {
//     throughput::<false>();
// }

#[test]
fn bench_zipf() {
    throughput::<true>();
}

/// Spawns a constant number of threads and schedules a constant number of tasks on each of them,
/// with each task containing a constant number of read/write operations.
///
/// See [`spawn_bench_task`] for more information.
fn throughput<const ZIPF: bool>() {
    BufferPoolManager::initialize(FRAMES, STORAGE_PAGES);

    COUNTER.store(0, Ordering::Release);
    println!("Operations: {OPERATIONS}");

    let start = std::time::Instant::now();
    let barrier = Arc::new(Barrier::new(THREADS * TASKS));

    thread::scope(|s| {
        // Spawn all threads with tasks on them.
        for thread in 0..THREADS {
            let barrier = barrier.clone();
            s.spawn(move || {
                let core_id = CoreId { id: thread };
                assert!(core_affinity::set_for_current(core_id));

                BufferPoolManager::start_thread(async move {
                    // Spawn all of the tasks lazily.
                    let mut set = JoinSet::new();
                    for task in 0..TASKS {
                        let handle = spawn_bench_task::<ZIPF>(barrier.clone(), thread, task);
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

        // Spawn a counter thread that reports metrics every second.
        s.spawn(move || {
            let second = std::time::Duration::from_secs(1);
            let mut counter = 0;

            // Only start counting once the first operation has occured.
            while COUNTER.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }

            while counter < THREADS * TASKS * ITERATIONS {
                let prev = counter;
                counter = COUNTER.load(Ordering::Acquire);

                println!("Operations per second: {}", counter - prev);
                std::thread::sleep(second);
            }

            let end = std::time::Instant::now();
            let elapsed = end - start;
            println!("Time elapsed: {:?}", elapsed);

            std::process::exit(0);
        });
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), THREADS * TASKS * ITERATIONS);
}

fn spawn_bench_task<const ZIPF: bool>(
    barrier: Arc<Barrier>,
    thread: usize,
    task: usize,
) -> JoinHandle<()> {
    let bpm = BufferPoolManager::get();

    let zipf = ZipfDistribution::new(STORAGE_PAGES, ZIPF_EXP).unwrap();
    let coin = Bernoulli::new(WRITE_RATIO).unwrap();
    let mut rng = rand::thread_rng();

    let task_name = format!("thread {} task {}", thread, task);
    BufferPoolManager::spawn_local(futures_diagnose::diagnose(task_name, async move {
        let mut handles = Vec::with_capacity(ITERATIONS);

        for _ in 0..ITERATIONS {
            let id = if ZIPF {
                zipf.sample(&mut rng)
            } else {
                rng.gen_range(0..STORAGE_PAGES)
            } as u64;

            let pid = PageId::new(id);
            let ph = bpm.get_page(&pid).unwrap();

            handles.push(ph);
        }

        // Wait for all tasks to finish setup.
        barrier.wait().await;

        for ph in handles {
            if coin.sample(&mut rng) {
                let mut write_guard = ph.write().await.unwrap();
                write_guard.deref_mut().fill(b'a');
                write_guard.flush().await.unwrap();
            } else {
                let read_guard = ph.read().await.unwrap();
                let slice = read_guard.deref();
                std::hint::black_box(slice);
            }

            COUNTER.fetch_add(1, Ordering::Release);
        }
    }))
}
