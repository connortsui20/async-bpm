use async_bpm::{page::PageId, BufferPoolManager};
use std::ops::DerefMut;
use std::thread;

#[test]
#[ignore]
fn test_single_thread() {
    BufferPoolManager::initialize(64, 128);
    let bpm = BufferPoolManager::get();

    let handle = thread::spawn(move || {
        let pid = PageId::new(0);
        let ph = bpm.get_page(&pid).unwrap();
        let mut guard = ph.write().unwrap();
        guard.deref_mut().fill(b'A');
        guard.flush().unwrap();
    });

    handle.join().unwrap();
}

#[test]
#[ignore]
fn test_basic() {
    const THREADS: usize = 8;

    BufferPoolManager::initialize(64, 256);
    let bpm = BufferPoolManager::get();

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            s.spawn(move || {
                    let index = 2 * i as u8;
                    let pid = PageId::new(index as u64);
                    let ph = bpm.get_page(&pid).unwrap();

                    let mut guard = ph.write().unwrap();
                    guard.deref_mut().fill(b' ' + index);
                    guard.flush().unwrap();

                    let index = ((2 * i) + 1) as u8;
                    let pid = PageId::new(index as u64);
                    let ph = bpm.get_page(&pid).unwrap();

                    let mut guard: async_bpm::page::WritePageGuard = ph.write().unwrap();
                    guard.deref_mut().fill(b' ' + index);
                    guard.flush().unwrap();
            });
        }
    });
}
