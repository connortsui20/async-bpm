//! An asynchronous buffer pool manager, built on top of `tokio` and `io_uring`.

#![cfg(target_family = "unix")]
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
#![warn(clippy::missing_safety_doc)]

pub mod bpm;
pub mod drive;
pub mod io;
pub mod page;
