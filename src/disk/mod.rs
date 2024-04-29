//! Implementation of functionality related to the management of data in between non-volatile /
//! permanent / disk storage and memory.

pub mod disk_manager;
pub(crate) mod eviction;
pub(crate) mod frame;
