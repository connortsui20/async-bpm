use async_bpm::io::IoUringAsync;
use async_bpm::{bpm::BufferPoolManager, page::PageId};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Builder;
use tokio::sync::Barrier;
use tokio::task::{self, spawn_local, LocalSet};

// #[test]
// fn test_bpm_register() {
//     let num_frames = 1 << 10;
//     let bpm = Arc::new(BufferPoolManager::new(num_frames));
//     assert_eq!(bpm.num_frames(), num_frames);

//     let id = PageId::new(0);

//     // Create the runtime
//     let rt = Builder::new_current_thread()
//         .on_thread_park(move || {
//             // let uring = bpm_clone.get_thread_local_uring().await;
//         })
//         .enable_all()
//         .build()
//         .unwrap();
//     let rt = Arc::new(rt);

//     let local = LocalSet::new();

//     let uring = bpm.get_thread_local_uring();
//     let uring_listener = uring.clone();
//     let uring_submitter = uring.clone();

//     rt.block_on(async move {
//         local
//             .run_until(async {
//                 let async_fd = Rc::new(AsyncFd::new(uring).unwrap());

//                 println!("Spawning listener");
//                 let listener =
//                     task::spawn_local(IoUringAsync::listener(uring_listener, async_fd.clone()));
//                 println!("Spawning submitter");
//                 let submitter =
//                     task::spawn_local(IoUringAsync::submitter(uring_submitter, async_fd.clone()));

//                 println!("Beginning Test");

//                 assert!(bpm.get_page(id).await.is_none());
//                 let _page_handle1 = bpm.create_page(id).await;

//                 for i in 0..10000 {
//                     std::hint::black_box(i);
//                     assert!(bpm.get_page(id).await.is_some());
//                     tokio::task::yield_now().await;
//                 }

//                 println!("Finishing Test");
//             })
//             .await;
//     });
// }

// #[test]
// fn test_new_bpm_with_threads() {
//     let num_frames = 1 << 15;
//     let bpm = Arc::new(BufferPoolManager::new(num_frames));
//     let bpm_clone = bpm.clone();

//     assert_eq!(bpm.num_frames(), num_frames);

//     let id1 = PageId::new(0);
//     let id2 = PageId::new(42);

//     // Create the runtime
//     let rt = Builder::new_current_thread()
//         .on_thread_park(move || {
//             // let uring = bpm_clone.get_thread_local_uring().await;
//         })
//         .enable_all()
//         .build()
//         .unwrap();
//     let rt = Arc::new(rt);

//     let barrier = Arc::new(Barrier::new(2));

//     // TODO add tasks that continuously poll and submit
//     thread::scope(|s| {
//         let bpm1 = bpm.clone();
//         let bpm2 = bpm.clone();
//         let rt1 = rt.clone();
//         let rt2 = rt.clone();
//         let barrier1 = barrier.clone();
//         let barrier2 = barrier.clone();

//         s.spawn(move || {
//             let local = LocalSet::new();

//             let bpm1_listener = bpm1.clone();
//             let bpm1_submitter = bpm1.clone();

//             local.spawn_local(async move {
//                 assert!(bpm1.get_page(id1).await.is_none());
//                 let _page_handle1 = bpm1.create_page(id1).await;
//                 assert!(bpm1.get_page(id1).await.is_some());

//                 barrier1.wait().await;

//                 for _ in 0..10000 {
//                     assert!(bpm1.get_page(id1).await.is_some());
//                     assert!(bpm1.get_page(id2).await.is_some());
//                 }
//             });

//             local.spawn_local(async move {
//                 let uring_listener = bpm1_listener.get_thread_local_uring().await;
//                 spawn_local(async move { uring_listener.listener().await });
//             });

//             local.spawn_local(async move {
//                 let uring_submitter = bpm1_submitter.get_thread_local_uring().await;
//                 spawn_local(async move { uring_submitter.submitter().await });
//             });

//             // This will return once all senders are dropped and all
//             // spawned tasks have returned.
//             rt1.block_on(local);
//         });

//         s.spawn(move || {
//             let local = LocalSet::new();

//             let bpm2_listener = bpm2.clone();
//             let bpm2_submitter = bpm2.clone();

//             local.spawn_local(async move {
//                 assert!(bpm2.get_page(id2).await.is_none());
//                 let _page_handle2 = bpm2.create_page(id2).await;
//                 assert!(bpm2.get_page(id2).await.is_some());

//                 barrier2.wait().await;

//                 for _ in 0..10000 {
//                     assert!(bpm2.get_page(id1).await.is_some());
//                     assert!(bpm2.get_page(id2).await.is_some());
//                 }
//             });

//             local.spawn_local(async move {
//                 let uring_listener = bpm2_listener.get_thread_local_uring().await;
//                 spawn_local(async move { uring_listener.listener().await });
//             });

//             local.spawn_local(async move {
//                 let uring_submitter = bpm2_submitter.get_thread_local_uring().await;
//                 spawn_local(async move { uring_submitter.submitter().await });
//             });

//             // This will return once all senders are dropped and all
//             // spawned tasks have returned.
//             rt2.block_on(local);
//         });
//     });
// }
