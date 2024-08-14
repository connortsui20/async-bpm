#![doc = include_str!("../README.md")]
#![cfg(target_family = "unix")]
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
#![warn(clippy::missing_safety_doc)]

pub mod bpm;
pub mod page;
pub(crate) mod storage;
