use crate::page::PageRef;
use std::io::IoSlice;

pub struct Frame {
    pub(crate) buf: IoSlice<'static>,
    pub(crate) parent: Option<PageRef>,
}

impl Frame {
    pub fn new(ioslice: IoSlice<'static>) -> Self {
        Self {
            buf: ioslice,
            parent: None,
        }
    }
}
