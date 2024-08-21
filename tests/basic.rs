use async_bpm::replacer::Fifo;
use async_bpm::BufferPoolManager;
use std::ops::DerefMut;
use std::sync::Arc;

#[test]
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
