use crate::page::PageRef;
use std::io::IoSlice;

/// An owned buffer frame, intended to be shared between user and kernel space.
#[derive(Debug)]
pub struct Frame {
    pub(crate) buf: IoSlice<'static>,
    pub(crate) owner: Option<PageRef>,
}

impl Frame {
    /// Creates a new and owned [`Frame`] given a static [`IoSlice`].
    pub fn new(ioslice: IoSlice<'static>) -> Self {
        Self {
            buf: ioslice,
            owner: None,
        }
    }
}
