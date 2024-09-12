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

const SECONDS: usize = 30;

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

const FIND_TASKS: usize = 32;
const FIND_THREADS: usize = 4;
const FIND_TASKS_PER_THREAD: usize = FIND_TASKS / FIND_THREADS;

const SCAN_TASKS: usize = 96;
const SCAN_THREADS: usize = 4;
const SCAN_TASKS_PER_THREAD: usize = SCAN_TASKS / SCAN_THREADS;

const TOTAL_TASKS: usize =
    (if WRITE { FIND_TASKS } else { 0 }) + (if READ { SCAN_TASKS } else { 0 });

const TOTAL_THREADS: usize =
    (if WRITE { FIND_THREADS } else { 0 }) + (if READ { SCAN_THREADS } else { 0 });

const RANDOM_OPERATIONS: usize = 1 << 24;

const TASK_ACCESSES: usize = RANDOM_OPERATIONS / FIND_TASKS;

const FRAMES: usize = GIGABYTE_PAGES;
const STORAGE_PAGES: usize = 32 * GIGABYTE_PAGES;

const ZIPF_EXP: f64 = 1.1;

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static SCAN_COUNTER: AtomicUsize = AtomicUsize::new(0);

const WRITE: bool = true;
const READ: bool = true;

/// Spawns a constant number of threads and schedules a constant number of tasks on each of them,
/// with each task containing a constant number of read/write operations.
///
/// See [`spawn_bench_task`] for more information.
#[test]
#[ignore]
fn throughput() {
    BufferPoolManager::initialize(FRAMES, STORAGE_PAGES);

    let start = std::time::Instant::now();
    let barrier = Arc::new(Barrier::new(TOTAL_TASKS));

    thread::scope(|s| {
        // Spawn all get threads with tasks on them.
        if WRITE {
            for thread in 0..FIND_THREADS {
                let barrier = barrier.clone();
                s.spawn(move || {
                    let core_id = CoreId { id: thread };
                    assert!(core_affinity::set_for_current(core_id));

                    BufferPoolManager::start_thread(async move {
                        // Spawn all of the tasks lazily.
                        let mut set = JoinSet::new();
                        for _task in 0..FIND_TASKS_PER_THREAD {
                            let handle = spawn_find_task(barrier.clone());
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
        }

        // Spawn all scan threads with tasks on them.
        if READ {
            for scan_thread in 0..SCAN_THREADS {
                let barrier = barrier.clone();
                s.spawn(move || {
                    let id = if WRITE {
                        FIND_THREADS + scan_thread
                    } else {
                        scan_thread
                    };
                    let core_id = CoreId { id };
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
        }

        // Spawn a counter thread that reports metrics every second.
        s.spawn(move || {
            // Will unfortunately be pinned to one of the scan threads.
            let core_id = CoreId {
                id: TOTAL_THREADS - 1,
            };
            assert!(core_affinity::set_for_current(core_id));

            let second_duration = std::time::Duration::from_secs(1);
            let mut get_counter = 0;
            let mut scan_counter = 0;
            let mut io_counter = 0;

            if WRITE {
                while COUNTER.load(Ordering::Relaxed) == 0 {
                    std::hint::spin_loop();
                }
            }

            for _ in 0..SECONDS {
                let prev_get = get_counter;
                let prev_scan = scan_counter;
                let prev_io = io_counter;

                get_counter = COUNTER.load(Ordering::Acquire);
                scan_counter = SCAN_COUNTER.load(Ordering::Acquire);
                io_counter = IO_OPERATIONS.load(Ordering::Acquire);

                let get_ops = get_counter - prev_get;
                let scan_ops = scan_counter - prev_scan;
                let io_ops = io_counter - prev_io;

                println!("{},{},{}", get_ops, scan_ops, io_ops);

                std::thread::sleep(second_duration);
            }

            let end = std::time::Instant::now();
            let elapsed = end - start;
            println!("Time elapsed: {:?}", elapsed);

            std::process::exit(0);
        });
    });
}

fn spawn_find_task(barrier: Arc<Barrier>) -> JoinHandle<()> {
    let bpm = BufferPoolManager::get();

    // Since half of the threads are solely reading, we double the writers here.
    let zipf = ZipfDistribution::new(STORAGE_PAGES, ZIPF_EXP).unwrap();
    let mut rng = rand::thread_rng();

    BufferPoolManager::spawn_local(async move {
        let mut handles = Vec::with_capacity(TASK_ACCESSES);

        for _ in 0..TASK_ACCESSES {
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
