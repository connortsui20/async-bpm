use async_bpm::{
    page::{PageId, PAGE_SIZE},
    BufferPoolManager, IO_OPERATIONS,
};
use rand::{distributions::Distribution, thread_rng, Rng};
use std::sync::Barrier;
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};
use zipf::ZipfDistribution;

const SECONDS: usize = 30;

const GIGABYTE: usize = 1024 * 1024 * 1024;
const GIGABYTE_PAGES: usize = GIGABYTE / PAGE_SIZE;

const FIND_THREADS: usize = 32;
const SCAN_THREADS: usize = 96;

const RANDOM_OPERATIONS: usize = 1 << 24;
const TASK_ACCESSES: usize = RANDOM_OPERATIONS / FIND_THREADS;

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
    let barrier = Arc::new(Barrier::new(FIND_THREADS + SCAN_THREADS));

    thread::scope(|s| {
        // Spawn all threads with tasks on them.
        for _thread in 0..FIND_THREADS {
            let barrier = barrier.clone();
            s.spawn(move || {
                spawn_bench_task(barrier.clone());
            });
        }

        for _thread in 0..SCAN_THREADS {
            let barrier = barrier.clone();
            s.spawn(move || {
                spawn_scan_task(barrier.clone());
            });
        }

        // Spawn a counter thread that reports metrics every second.
        s.spawn(move || {
            let second = std::time::Duration::from_secs(1);
            let mut get_counter = 0;
            let mut scan_counter = 0;
            let mut io_counter = 0;

            // Only start counting once the first operation has occured.
            while COUNTER.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
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

                std::thread::sleep(second);
            }

            let end = std::time::Instant::now();
            let elapsed = end - start;
            println!("Time elapsed: {:?}", elapsed);

            std::process::exit(0);
        });
    });
}

fn spawn_bench_task(barrier: Arc<Barrier>) {
    let bpm = BufferPoolManager::get();

    let zipf = ZipfDistribution::new(STORAGE_PAGES, ZIPF_EXP).unwrap();
    let mut rng = rand::thread_rng();

    let mut handles = Vec::with_capacity(TASK_ACCESSES);

    for _ in 0..TASK_ACCESSES {
        let id = zipf.sample(&mut rng);

        let pid = PageId::new(id as u64);
        let ph = bpm.get_page(&pid).unwrap();

        handles.push(ph);
    }

    // Wait for all tasks to finish setup.
    barrier.wait();

    for ph in handles {
        let mut write_guard = ph.write().unwrap();
        write_guard.deref_mut().fill(b'a');
        write_guard.flush().unwrap();

        COUNTER.fetch_add(1, Ordering::Release);
    }
}

fn spawn_scan_task(barrier: Arc<Barrier>) {
    let bpm = BufferPoolManager::get();

    barrier.wait();

    let mut rng = thread_rng();
    let start = rng.gen_range(0..STORAGE_PAGES);

    // Continuously scan all pages.
    loop {
        for i in 0..STORAGE_PAGES / 2 {
            let pid = PageId::new(((i + start) % STORAGE_PAGES) as u64);
            let ph = bpm.get_page(&pid).unwrap();
            let read_guard = ph.read().unwrap();
            let slice = read_guard.deref();
            std::hint::black_box(slice);

            SCAN_COUNTER.fetch_add(1, Ordering::Release);
        }
    }
}
