use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::sync::Arc;
use std::thread;
use tokio::sync::Barrier;
use tokio::task::{spawn_local, LocalSet};

#[tokio::test]
async fn test_new_bpm() {
    let num_frames = 1 << 15;
    let bpm = Arc::new(BufferPoolManager::new(num_frames));

    assert_eq!(bpm.num_frames(), num_frames);

    let id1 = PageId::new(0);
    let id2 = PageId::new(42);

    // TODO add tasks that continuously poll and submit
    assert!(bpm.get_page(id1).await.is_none());
    let _page_handle1 = bpm.create_page(id1).await;
    assert!(bpm.get_page(id1).await.is_some());

    assert!(bpm.get_page(id2).await.is_none());
    let _page_handle2 = bpm.create_page(id2).await;
    assert!(bpm.get_page(id2).await.is_some());
}

#[test]
fn test_new_bpm_with_threads() {
    use tokio::runtime::Builder;

    let num_frames = 1 << 15;
    let bpm = Arc::new(BufferPoolManager::new(num_frames));
    let bpm_clone = bpm.clone();

    assert_eq!(bpm.num_frames(), num_frames);

    let id1 = PageId::new(0);
    let id2 = PageId::new(42);

    // Create the runtime
    let rt = Builder::new_current_thread()
        .on_thread_park(move || {
            // let uring = bpm_clone.get_thread_local_uring().await;
        })
        .enable_all()
        .build()
        .unwrap();
    let rt = Arc::new(rt);

    let barrier = Arc::new(Barrier::new(2));

    // TODO add tasks that continuously poll and submit
    thread::scope(|s| {
        let bpm1 = bpm.clone();
        let bpm2 = bpm.clone();
        let rt1 = rt.clone();
        let rt2 = rt.clone();
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();

        s.spawn(move || {
            let local = LocalSet::new();

            let bpm1_listener = bpm1.clone();
            let bpm1_submitter = bpm1.clone();

            local.spawn_local(async move {
                assert!(bpm1.get_page(id1).await.is_none());
                let _page_handle1 = bpm1.create_page(id1).await;
                assert!(bpm1.get_page(id1).await.is_some());

                barrier1.wait().await;

                for _ in 0..10000 {
                    assert!(bpm1.get_page(id1).await.is_some());
                    assert!(bpm1.get_page(id2).await.is_some());
                }
            });

            local.spawn_local(async move {
                let uring_listener = bpm1_listener.get_thread_local_uring().await;
                spawn_local(async move { uring_listener.listener().await });
            });

            local.spawn_local(async move {
                let uring_submitter = bpm1_submitter.get_thread_local_uring().await;
                spawn_local(async move { uring_submitter.submitter().await });
            });

            // This will return once all senders are dropped and all
            // spawned tasks have returned.
            rt1.block_on(local);
        });

        s.spawn(move || {
            let local = LocalSet::new();

            let bpm2_listener = bpm2.clone();
            let bpm2_submitter = bpm2.clone();

            local.spawn_local(async move {
                assert!(bpm2.get_page(id2).await.is_none());
                let _page_handle2 = bpm2.create_page(id2).await;
                assert!(bpm2.get_page(id2).await.is_some());

                barrier2.wait().await;

                for _ in 0..10000 {
                    assert!(bpm2.get_page(id1).await.is_some());
                    assert!(bpm2.get_page(id2).await.is_some());
                }
            });

            local.spawn_local(async move {
                let uring_listener = bpm2_listener.get_thread_local_uring().await;
                spawn_local(async move { uring_listener.listener().await });
            });

            local.spawn_local(async move {
                let uring_submitter = bpm2_submitter.get_thread_local_uring().await;
                spawn_local(async move { uring_submitter.submitter().await });
            });

            // This will return once all senders are dropped and all
            // spawned tasks have returned.
            rt2.block_on(local);
        });
    });
}
