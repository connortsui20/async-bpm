# Asynchronous Buffer Pool Manager

An asynchronous buffer pool manager, built on top of [`tokio_uring`] and [`tokio`].

The goal of this buffer pool manager is to exploit parallelism as much as possible by eliminating
the use of any global locks/latches or any single points of contention for the entire system. This
means that several parts of this system are implemented quite differently to how a traditional
buffer pool manager would be implemented.

Most notable is the fact that I/O is non-blocking, courtesy of the `io_uring` Linux interface that
the [`tokio_uring`] interface is built on top of. The second most notable part of this system is
that there is no global page table that users must go through to access any page of data. All page
data and data movement (eviction) is _decentralized_. 

By making use of [`tokio_uring`], this buffer pool manager is implemented with a thread-per-core
model where many light-weight asynchronous tasks can be scheduled on each operating system provided
thread. And because the system is thread-per-core (and not a work-stealing system), it is the
responsibility of the caller to ensure that tasks are balanced between threads.

# Usage

The intended usage is as follows:
-   Call [`BufferPoolManager::initialize`] to set the memory and storage sizes
-   Spawn 1 operating system thread for every CPU core
-   Call [`BufferPoolManager::start_thread`] on each thread to initialize each for the buffer pool
-   For each of the tasks on each thread, call [`BufferPoolManager::spawn_local`]
-   To access a page of data, use [`PageHandle`](crate::page::PageHandle)s via
    [`BufferPoolManager::get_page`]
-   Use the [`read`](crate::page::PageHandle::read) or [`write`](crate::page::PageHandle::write)
    methods on [`PageHandle`](crate::page::PageHandle) and then perform the intended operations over
    the page's data

### Single-Threaded Example

Here is a simple example of starting the [`BufferPoolManager`] on a single thread (in this case,
the main thread). It accesses a single page from persistent storage, fills it with the character
`'A'`, and then writes it out to persistent storage.

```rust
use async_bpm::BufferPoolManager;
use async_bpm::page::PageId;
use std::ops::DerefMut;

// Initialize a buffer pool with 64 frames and 128 pages on peristent storage.
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
```

Since there should be only 1 [`BufferPoolManager`] instance at a time, the
[`BufferPoolManager::get`] method returns a `&'static` reference to a global instance. This
global buffer pool instance contains the global storage manager instance.

Note that the call to [`flush`](crate::page::WritePageGuard::flush) is not strictly
necessary, as it is only a way to force an immediate write out to persistent storage.

### Multi-Threaded Example

Here is a multi-threaded example on 8 threads, where each thread spawns 2 tasks that write a
unique character to persistent storage.

```rust
use async_bpm::BufferPoolManager;
use async_bpm::page::PageId;
use std::ops::DerefMut;

const THREADS: usize = 8;

BufferPoolManager::initialize(64, 256);
let bpm = BufferPoolManager::get();

// Spawn all threads
std::thread::scope(|s| {
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
```

### More Examples

TODO more examples.

<br>

# Design

This system is designed as a thread-per-core model with multiple persistent storage devices. The
reason that it is not a multi-threaded worker pool model like the [`tokio`] runtime is due to how
the `io_uring` Linux interface works. I/O operations on `io_uring` are submitted to a single
`io_uring` instance that is registered per-thread to eliminate contention. To move a task to a
separate thread would mean that the task could no longer poll the operation result off of the
thread-local `io_uring` completion queue. Thus, lightweight asynchronous tasks given to worker 
threads cannot be moved between threads (or in other words, are `!Send`). 

A consequence of this fact is that threads cannot work-steal in the same manner that the [`tokio`]
runtime allows threads to do. It is thus the responsibility of some global scheduler to assign tasks
to worker threads appropriately. Then, once a task has been given to a worker thread, the buffer
pool's internal thread-local scheduler (which is just a [`tokio_uring`] local scheduler) is in
charge of managing all of the cooperative tasks.

This buffer pool manager is heavily inspired by leanstore, which you can read about
[here](https://www.vldb.org/pvldb/vol16/p2090-haas.pdf), and future work could introduce the
all-to-all model of threads to distinct SSDs, where each worker thread has a dedicated `io_uring`
instance for every physical SSD.
