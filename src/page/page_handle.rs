use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::Page;
use std::sync::Arc;

pub struct PageHandle {
    page: Arc<Page>,
}

impl PageHandle {
    pub async fn read(&self) -> ReadPageGuard {
        self.page.read().await
    }

    pub async fn write(&self) -> WritePageGuard {
        self.page.write().await
    }
}
