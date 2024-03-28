use crate::page::PageRef;
use std::io::IoSlice;

#[derive(Debug)]
pub struct Frame {
    pub(crate) buf: IoSlice<'static>,
    pub(crate) parent: Option<PageRef>,
}

impl Frame {
    pub fn new(slice: &'static mut [u8]) -> Self {
        let iovec = IoSlice::new(slice);
        Self {
            buf: iovec,
            parent: None,
        }
    }
}
