use crate::page::PageRef;
use std::{
    io::IoSliceMut,
    ops::{Deref, DerefMut},
};

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    /// The buffer that this [`Frame`] holds ownership over.
    ///
    /// Since [`Frame`] is not [`Clone`]able, this [`Frame`] is guaranteed to have sole access to
    /// the inner [`IoSliceMut`].
    pub(crate) buf: IoSliceMut<'static>,

    /// A reference to the page that owns this [`Frame`], if an owner actually exists
    pub(crate) owner: Option<PageRef>,
}

impl Frame {
    /// Creates a new and owned [`Frame`] given a static [`IoSliceMut`].
    pub fn new(ioslice: IoSliceMut<'static>) -> Self {
        Self {
            buf: ioslice,
            owner: None,
        }
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf.deref()
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.deref_mut()
    }
}
