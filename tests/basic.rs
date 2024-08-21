use async_bpm::replacer::Fifo;
use async_bpm::{page::PageId, BufferPoolManager};
use std::ops::DerefMut;
use std::thread;

#[test]
#[ignore]
fn test_single_thread() {
    let bpm = BufferPoolManager::<Fifo>::new(64, 128);

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
