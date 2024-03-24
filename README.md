# Design

This model is aimed at a thread-per-core model with a single logical disk.
This implies that tasks (coroutines) given to worker threads cannot be moved between threads
(or in other words, are `!Send`).
So it is on a global scheduler to assign tasks to worker threads appropriately.
Once a task has been given to a worker thread, then the asynchronous runtime's
scheduler is in charge of managing the cooperative tasks.

An implication of the above is that this model will not work with `tokio`'s work-stealing multi-threaded runtime.
However, the benefits of parallelism in this model at the cost of having to manually manage load balancing
is likely worth it. Additionally, a DBMS that could theoretically use this model would likely have
better knowledge of how to schedule things appropriately.

Finally, this is heavily inspired by [this Leanstore paper](https://www.vldb.org/pvldb/vol16/p2090-haas.pdf),
and future work could introduce the all-to-all model of threads to distinct SSDs,
where each worker thread has a dedicated `io_uring` instance for every physical SSD.

# Objects and Types

## Thread Locals

-   `PageHandle`: A shared pointer to a `Page` (through an `Arc`)
-   Local `io_uring` instances (that are `!Send`)
-   Slab-allocated futures defining the lifecycle of an `io_uring` event (private)

## Shared Objects

-   Shared pre-registered buffers / frames
    -   Frames are owned types (can only belong to a specific `Page`)
    -   Frames also have pointers back to their parent `Page`s
-   Shared free list of frames (via a concurrent queue)
-   `Page`: A hybrid-latched (read-write locked for now) page header
    -   State is either `Unloaded`, `Loading` (private), or `Loaded`
        -   `Unloaded` implies that the data is not in memory
        -   `Loading` implies that the data is being loaded from disk, and contains a future (private)
        -   `Loaded` implies the data is on one of the pre-registered buffers, and owns a registered buffer
    -   `Page`s also have eviction state
        -   `Hot` implies that this is a frequently-accessed page
        -   `Cool` implies that it is not frequently accessed, and might be evicted soon

Note that the eviction state is really just an optimization for making a decision on pages to evict.
The page eviction state _does not_ imply anything about the state of a page
(so a page could be `Hot` and also `Unloaded`), and all accesses to a page must still go through the hybrid latch.

In summary, the possible states that the `Page` can be in is:

-   `Loaded` and `Hot` (frequently accessed)
-   `Loaded` and `Cool` (potential candidate for eviction)
-   `Loading` (`Hot`/`Cool` plus private `io_uring` event state)
-   `Unloaded` (`Hot`/`Cool`)

# Algorithms

### Write Access Algorithm

Let P1 be the page we want to get write access for.

-   Set eviction state to `Hot`
-   Write-lock P1
-   If `Loaded` (SEMI-HOT PATH):
    -   Modify the page data
    -   Unlock and return
-   Else `Unloaded`, and we need to load the page
    -   Load a page via the [load algorithm](#load-algorithm)
    -   Modify the page data
    -   Unlock and return

### Read Access Algorithm

Let P1 be the page we want to get read access for. All optimistic reads have to be done through a closure.

-   Set eviction state to `Hot`
-   Optimistically read P1
-   If `Loaded` (HOT PATH):
    -   Read the page data optimistically and return
    -   If the optimistic read fails, fallback to a pessimistic read
        -   If `Loaded` (SEMI-HOT PATH):
            -   Read normally and return
        -   Else it is now `Unloaded`, so continue
-   The state is `Unloaded`, and we need to load a page
    -   Upgrade the read lock into a write lock (either drop and retake, or directly upgrade)
    -   Load a page via the [load algorithm](#load-algorithm)
    -   Read the page data
    -   Unlock and return

### Load algorithm

Let P1 be the page we want to load from disk into memory. The caller must have the write lock on P1.
Once this algorithm is complete, the page is guaranteed to be loaded into the owned frame,
and the page eviction state will be `Hot`.

-   If the page is `Loaded`, then immediately return
-   Otherwise, this page is `Unloaded`
-   Check for free frames in the global free list
    -   If there is a free frame, take ownership of the frame
    -   Set the frame's parent pointer to P1
    -   Read P1's data from disk into the buffer
    -   `await` read completion from the local `io_uring` instance
-   Else there are no free frames, and we need to evict a page
    -   Continue with the [Frame Transfer Algorithm](#frame-transfer-algorithm)
-   At the end, set the page eviction state to `Hot`

### Frame Transfer Algorithm

Let P1 be the page we want to load in from disk, and P2 be the page we will want to evict.
The caller must also hold a write-lock on P1.

Note that the random choice of a `Cool` page P2 into write locking P2 could potentially mean that
other threads could make P2 `Hot` while the current thread is attempting to grab the write lock.

We will make the decision to not allow readers to preempt writers.
If P2 is actually a very frequently accessed page, then there was a low probability that we decided to
evict it in the first place, and the future accessors will bring it back into memory anyways later.

-   Write-lock P1
-   Set P1 to `Loading`
-   Find a page to evict by looping over all `Cool` frames and picking a random one (we pick P2)
-   Write-lock P2
-   Write P2's buffer data out to disk via the local `io_uring` instance
-   `await` write completion from the local `io_uring` instance
-   Set P2 to `Unloaded`
-   Give ownership of P2's frame to P1
-   Set the frame's parent pointer to P1
-   Unlock P2
-   Read P1's data from disk into the buffer via the local `io_uring` instance
-   `await` read completion from the local `io_uring` instance
-   Set P1 to `Loaded`

### General Eviction Algorithm

On every thread, there will be at least 1 "background" task (not scheduled by the global scheduler)
dedicated to evicting pages. It will aim to have some certain threshold of free pages in the free list.

-   Iterate over all frames
-   Collect the list of `Page`s that are `Loaded` (should not be more than the number of frames)
-   Collect all `Cool` pages
-   For every `Page` that is `Hot`, change to `Cool`
-   Randomly choose some (small) constant number of pages from the list of initially `Cool`
-   `join!` the following page eviction futures:
    -   For each page Px we want to evict:
        -   Check if Px has been changed to `Hot`, and if so, return early
        -   Write-lock Px
        -   If Px is now either `Hot` or `Unloaded`, unlock and return early
        -   Write Px's buffer data out to disk via the local `io_uring` instance
        -   `await` write completion from the local `io_uring` instance
        -   Set Px to `Unloaded`
        -   Give ownership of Px's frame to the global free list of frames
        -   Unlock Px
