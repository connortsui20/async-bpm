use crate::page::{Page, PAGE_SIZE};
use std::{
    ops::{Deref, DerefMut, Range},
    sync::Arc,
};
use tokio::sync::Mutex;
use tokio_uring::buf::BoundedBuf;

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
    group_id: usize,
    eviction_states: Mutex<[EvictionState; FRAME_GROUP_SIZE]>,
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

impl BoundedBuf for Frame {
    type Buf = &'static [u8];

    type Bounds = Range<usize>;

    fn slice(self, range: impl std::ops::RangeBounds<usize>) -> tokio_uring::buf::Slice<Self::Buf> {
        todo!()
    }

    fn slice_full(self) -> tokio_uring::buf::Slice<Self::Buf> {
        todo!()
    }

    fn get_buf(&self) -> &Self::Buf {
        todo!()
    }

    fn bounds(&self) -> Self::Bounds {
        Range {
            start: 0,
            end: PAGE_SIZE,
        }
    }

    fn from_buf_bounds(buf: Self::Buf, bounds: Self::Bounds) -> Self {
        todo!("Do we need to support this?")
    }

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
