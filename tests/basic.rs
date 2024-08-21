use async_bpm::replacer::Fifo;
use async_bpm::BufferPoolManager;
use std::ops::DerefMut;
use std::sync::Arc;
use std::thread;

#[test]
#[ignore]
fn test_single_thread() {
    let bpm = BufferPoolManager::<Fifo>::new(64, 128);
    let bpm = Arc::new(bpm);

    BufferPoolManager::<Fifo>::start_thread(async move {
        let ph = bpm.new_page().await.unwrap();

        {
            let mut guard = ph.write().await;
            guard.deref_mut().fill(b'A');
            guard.flush().await.unwrap();
        }
    });
}

#[test]
#[ignore]
fn test_basic() {
    const THREADS: usize = 8;

    let bpm = BufferPoolManager::<Fifo>::new(64, 128);
    let bpm = Arc::new(bpm);

    // Spawn all threads
    thread::scope(|s| {
        for i in 0..THREADS {
            let bpm = bpm.clone();
            s.spawn(move || {
                let bpm = bpm.clone();
                BufferPoolManager::<Fifo>::start_thread(async move {
                    let h1 = BufferPoolManager::<Fifo>::spawn_local(async move {
                        let pid = 2 * i;
                        let ph = bpm.get_page(&pid).await.unwrap();

                        {
                            let mut guard = ph.write().await;
                            guard.deref_mut().fill(b' ' + pid as u8);
                            guard.flush().await.unwrap();
                        }
                    });

                    let h2 = BufferPoolManager::<Fifo>::spawn_local(async move {
                        let pid = (2 * i) + 1;
                        let ph = bpm.get_page(&pid).await.unwrap();

                        {
                            let mut guard: async_bpm::page::WritePageGuard = ph.write().await;
                            guard.deref_mut().fill(b' ' + pid as u8);
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
