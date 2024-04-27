//! Implementation of I/O operations and functionality based on the linux `io_uring` interface.

pub(crate) mod op;
pub(crate) mod uring_async;

pub use uring_async::IoUringAsync;
pub use uring_async::IO_URING_DEFAULT_ENTRIES;
