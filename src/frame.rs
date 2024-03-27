use crate::page::Page;
use std::io::IoSlice;
use std::sync::Arc;

pub struct Frame {
    pub(crate) buf: IoSlice<'static>,
    pub(crate) parent: Option<Arc<Page>>,
}

impl Frame {
    pub fn new(slice: &'static mut [u8]) -> Self {
        let iovec = IoSlice::new(slice);
        Self {
            buf: iovec,
            parent: None,
        }
    }

    pub fn take_parent(&mut self) -> Option<Arc<Page>> {
        todo!()
    }

    pub fn replace_parent(&mut self, new_parent: Arc<Page>) -> Option<Arc<Page>> {
        todo!()
    }
}
