use crate::page::Page;
use std::sync::Arc;

pub struct Frame {
    buf: Vec<u8>, // TODO fix
    parent: Option<Arc<Page>>,
}
