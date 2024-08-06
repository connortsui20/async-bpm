//! This module contains the definition and implementation of [`Frame`] and [`FrameGroup`], which
//! are types that represent the buffer frames that the buffer pool manager is in charge of.
//!
//! A [`Frame`] is intended to hold [`PAGE_SIZE`] bytes of data, and is also intended to be shared
//! with the the kernel to avoid unnecessary `memcpy`s from the kernel's internal buffers into
//! user-space buffers.
//!
//! A [`FrameGroup`] instance groups [`Frame`]s together so that eviction algorithms can be run on
//! pre-determined groups of frames without having to manage which logical pages are in memory or
//! not in memory.

use crate::page::{Page, PAGE_SIZE};
use async_channel::{Receiver, Sender};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::Mutex;
use tokio_uring::buf::{IoBuf, IoBufMut};

/// The number of frames in a [`FrameGroup`].
pub const FRAME_GROUP_SIZE: usize = 64;

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    // TODO is this necessary
    frame_id: usize,

    /// The buffer that this `Frame` holds ownership over.
    ///
    /// Since `Frame` is not [`Clone`]able, this `Frame` is guaranteed to have exclusive access to
    /// the mutable buffer.
    buf: &'static mut [u8],
}

/// A fixed group of frames.
#[derive(Debug)]
pub(crate) struct FrameGroup {
    /// TODO docs
    group_id: usize,

    /// The states of the [`Frame`]s that belong to this `FrameGroup`.
    ///
    /// Only 1 thread is allowed to modify eviction states at any time, thus we protect them with an
    /// asynchronous [`Mutex`].
    eviction_states: Mutex<[EvictionState; FRAME_GROUP_SIZE]>,

    /// An asynchronous channel of free [`Frame`]s.
    free_frames: (Sender<Frame>, Receiver<Frame>),
}

/// The enum representing the possible values for [`EvictionState`].
///
/// The reason this is separate from the [`EvictionState`] struct is because we cannot represent do
/// atomic operations on enums in Rust.
#[derive(Debug)]
pub(crate) enum EvictionState {
    /// Represents a frequently / recently accessed [`Frame`](super::frame::Frame) that currently
    /// holds a [`Page`](crate::page::Page)'s data.
    Hot(Arc<Page>),
    /// Represents an infrequently or old [`Frame`](super::frame::Frame) that might be evicted soon,
    /// and also still currently holds a [`Page`](crate::page::Page)'s data.
    Cool(Arc<Page>),
    /// Represents a [`Frame`](super::frame::Frame) that does not hold any
    /// [`Page`](crate::page::Page)'s data.
    Cold,
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}

/// # Safety
///
/// The safety contract for `IoBuf` is as follows:
/// > Buffers passed to `io-uring` operations must reference a stable memory region. While the
/// > runtime holds ownership to a buffer, the pointer returned by `stable_ptr` must remain valid
/// > even if the `IoBuf` value is moved.
///
/// Since we only use a static reference to correctly allocated memory, all operations are safe.
unsafe impl IoBuf for Frame {
    fn stable_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        PAGE_SIZE
    }

    fn bytes_total(&self) -> usize {
        PAGE_SIZE
    }
}

/// # Safety
///
/// The safety contract for `IoBufMut` is as follows:
/// > Buffers passed to `io-uring` operations must reference a stable memory region. While the
/// > runtime holds ownership to a buffer, the pointer returned by `stable_mut_ptr` must remain
/// > valid even if the `IoBufMut` value is moved.
///
/// Since we only use a static reference to correctly allocated memory, all operations are safe.
unsafe impl IoBufMut for Frame {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, _pos: usize) {
        // All bytes are initialized on allocation, so this function is a no-op.
    }
}
