use crate::page::PageRef;
use std::io::IoSlice;

/// An owned buffer frame, intended to be shared between user and kernel space.
pub struct Frame {
    pub(crate) buf: IoSlice<'static>,
    pub(crate) parent: Option<PageRef>,
}

impl Frame {
    /// Creates a new and owned [`Frame`] given a static [`IoSlice`].
    pub fn new(ioslice: IoSlice<'static>) -> Self {
        Self {
            buf: ioslice,
            parent: None,
        }
    }
}
