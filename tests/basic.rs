use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::ops::DerefMut;
use std::thread;

#[test]
#[ignore]
fn test_single_thread() {
    BufferPoolManager::initialize(64, 128);
    let bpm = BufferPoolManager::get();

    BufferPoolManager::start_thread(async move {
        let pid = PageId::new(0);
        let ph = bpm.get_page(&pid).unwrap();

        {
            let mut guard = ph.write().await.unwrap();
            guard.deref_mut().fill(b'A');
            guard.flush().await.unwrap();
        }
    });
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
                BufferPoolManager::start_thread(async move {
                    let h1 = BufferPoolManager::spawn_local(async move {
                        let index = 2 * i as u8;
                        let pid = PageId::new(index as u64);
                        let ph = bpm.get_page(&pid).unwrap();

                        {
                            let mut guard = ph.write().await.unwrap();
                            guard.deref_mut().fill(b' ' + index);
                            guard.flush().await.unwrap();
                        }
                    });

                    let h2 = BufferPoolManager::spawn_local(async move {
                        let index = ((2 * i) + 1) as u8;
                        let pid = PageId::new(index as u64);
                        let ph = bpm.get_page(&pid).unwrap();

                        {
                            let mut guard: async_bpm::page::WritePageGuard =
                                ph.write().await.unwrap();
                            guard.deref_mut().fill(b' ' + index);
                            guard.flush().await.unwrap();
                        }
                    });

                    let (res1, res2) = tokio::join!(h1, h2);
                    res1.unwrap();
                    res2.unwrap();
                });
            });
        }
    });
}
