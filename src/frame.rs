use crate::page::{Page, PAGE_SIZE};
use std::sync::Arc;

pub struct Frame {
    pub(crate) buf: Vec<u8>, // TODO fix
    pub(crate) parent: Option<Arc<Page>>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            buf: vec![0; PAGE_SIZE],
            parent: None,
        }
    }
}
