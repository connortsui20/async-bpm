use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::sync::Arc;

#[tokio::test]
async fn test_new_bpm() {
    let num_frames = 1 << 22;
    let bpm = Arc::new(BufferPoolManager::new(num_frames));

    assert_eq!(bpm.num_frames(), num_frames);

    let id1 = PageId::new(0);
    let id2 = PageId::new(42);

    assert!(bpm.get_page(id1).await.is_none());
    let page_handle1 = bpm.create_page(id1).await;
    assert!(bpm.get_page(id1).await.is_some());

    assert!(bpm.get_page(id2).await.is_none());
    let page_handle2 = bpm.create_page(id2).await;
    assert!(bpm.get_page(id2).await.is_some());
}
