use async_bpm::{
    page::{PageId, PAGE_SIZE},
    BufferPoolManager, IO_OPERATIONS,
};
use core_affinity::CoreId;
use rand::thread_rng;
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

const GET_TASKS: usize = 32;
const GET_THREADS: usize = 8;
const GET_TASKS_PER_THREAD: usize = GET_TASKS / GET_THREADS;

const SCAN_TASKS: usize = 96;
const SCAN_THREADS: usize = 24;
const SCAN_TASKS_PER_THREAD: usize = SCAN_TASKS / SCAN_THREADS;

const OPERATIONS: usize = 1 << 26;
const THREAD_OPERATIONS: usize = OPERATIONS / GET_THREADS;
const ITERATIONS: usize = THREAD_OPERATIONS / GET_TASKS_PER_THREAD; // iterations per task

const FRAMES: usize = GIGABYTE_PAGES;
const STORAGE_PAGES: usize = 32 * GIGABYTE_PAGES;

const ZIPF_EXP: f64 = 1.1;

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static SCAN_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Spawns a constant number of threads and schedules a constant number of tasks on each of them,
/// with each task containing a constant number of read/write operations.
///
/// See [`spawn_bench_task`] for more information.
#[test]
#[ignore]
fn throughput() {
    BufferPoolManager::initialize(FRAMES, STORAGE_PAGES);

    let start = std::time::Instant::now();
    let barrier = Arc::new(Barrier::new(GET_TASKS + SCAN_TASKS));

    thread::scope(|s| {
        // Spawn all get threads with tasks on them.
        for thread in 0..GET_THREADS {
            let barrier = barrier.clone();
            s.spawn(move || {
                let core_id = CoreId { id: thread };
                assert!(core_affinity::set_for_current(core_id));

                BufferPoolManager::start_thread(async move {
                    // Spawn all of the tasks lazily.
                    let mut set = JoinSet::new();
                    for _task in 0..GET_TASKS_PER_THREAD {
                        let handle = spawn_bench_task(barrier.clone());
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

        // Spawn all scan threads with tasks on them.
        for scan_thread in 0..SCAN_THREADS {
            let barrier = barrier.clone();
            s.spawn(move || {
                let core_id = CoreId {
                    id: GET_THREADS + scan_thread,
                };
                assert!(core_affinity::set_for_current(core_id));

                BufferPoolManager::start_thread(async move {
                    // Spawn all of the tasks lazily.
                    let mut set = JoinSet::new();
                    for _task in 0..SCAN_TASKS_PER_THREAD {
                        let handle = spawn_scan_task(barrier.clone());
                        set.spawn(handle);
                    }

                    // Execute all tasks concurrently.
                    while let Some(res) = set.join_next().await {
                        let inner_res = res.unwrap();
                        inner_res.unwrap();
                    }
                })
            });
        }

        // Spawn a counter thread that reports metrics every second.
        s.spawn(move || {
            // Will unfortunately be pinned to one of the scan threads.
            let core_id = CoreId {
                id: GET_THREADS + SCAN_THREADS - 1,
            };
            assert!(core_affinity::set_for_current(core_id));

            let second = std::time::Duration::from_secs(1);
            let mut get_counter = 0;
            let mut scan_counter = 0;
            let mut io_counter = 0;

            // Only start counting once the first operation has occured.
            while COUNTER.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }

            while get_counter < GET_THREADS * GET_TASKS_PER_THREAD * ITERATIONS {
                let prev_get = get_counter;
                let prev_scan = scan_counter;
                let prev_io = io_counter;

                get_counter = COUNTER.load(Ordering::Acquire);
                scan_counter = SCAN_COUNTER.load(Ordering::Acquire);
                io_counter = IO_OPERATIONS.load(Ordering::Acquire);

                let get_ops = get_counter - prev_get;
                let scan_ops = scan_counter - prev_scan;
                let io_ops = io_counter - prev_io;

                println!("{},{},{},{}", get_ops, scan_ops, get_ops + scan_ops, io_ops);

                std::thread::sleep(second);
            }

            let end = std::time::Instant::now();
            let elapsed = end - start;
            println!("Time elapsed: {:?}", elapsed);

            std::process::exit(0);
        });
    });

    assert_eq!(COUNTER.load(Ordering::SeqCst), GET_TASKS * ITERATIONS);
}

fn spawn_bench_task(barrier: Arc<Barrier>) -> JoinHandle<()> {
    let bpm = BufferPoolManager::get();

    // Since half of the threads are solely reading, we double the writers here.
    let zipf = ZipfDistribution::new(STORAGE_PAGES, ZIPF_EXP).unwrap();
    let mut rng = rand::thread_rng();

    BufferPoolManager::spawn_local(async move {
        let mut handles = Vec::with_capacity(ITERATIONS);

        for _ in 0..ITERATIONS {
            let id = zipf.sample(&mut rng);

            let pid = PageId::new(id as u64);
            let ph = bpm.get_page(&pid).unwrap();

            handles.push(ph);
        }

        // Wait for all tasks to finish setup.
        barrier.wait().await;

        for ph in handles {
            let mut write_guard = ph.write().await.unwrap();
            write_guard.deref_mut().fill(b'a');
            write_guard.flush().await.unwrap();

            COUNTER.fetch_add(1, Ordering::Release);
        }
    })
}

fn spawn_scan_task(barrier: Arc<Barrier>) -> JoinHandle<()> {
    let bpm = BufferPoolManager::get();

    BufferPoolManager::spawn_local(async move {
        barrier.wait().await;

        let mut rng = thread_rng();
        let start = rng.gen_range(0..STORAGE_PAGES);

        // Continuously scan all pages.
        loop {
            for i in 0..STORAGE_PAGES / 2 {
                let pid = PageId::new(((i + start) % STORAGE_PAGES) as u64);
                let ph = bpm.get_page(&pid).unwrap();
                let read_guard = ph.read().await.unwrap();
                let slice = read_guard.deref();
                std::hint::black_box(slice);

                SCAN_COUNTER.fetch_add(1, Ordering::Release);
            }
        }
    })
}
