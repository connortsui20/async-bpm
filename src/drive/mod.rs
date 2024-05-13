//! Implementation of functionality related to the management of data in between persistent /
//! non-volatile storage and volatile memory.

pub mod drive_manager;
pub(crate) mod eviction;
pub(crate) mod frame;
