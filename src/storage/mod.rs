//! This module contains the definition and implementation of [`Frame`] and [`FrameGroup`], which
//! are types that represent the buffer frames that the buffer pool manager is in charge of.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`](crate::page::PAGE_SIZE) bytes of data, and is also
//! intended to be shared with the the kernel to avoid unnecessary `memcpy`s from the kernel's
//! internal buffers into user-space buffers.
//!
//! A [`FrameGroup`] instance groups [`Frame`]s together so that evictions do not have to search
//! every single [`Frame`] in the buffer pool for an eviction candidate.

mod frame;
mod frame_group;
mod storage_manager;

pub(crate) use frame::*;
pub(crate) use frame_group::*;
pub(crate) use storage_manager::*;
