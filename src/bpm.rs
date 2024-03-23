use std::{collections::HashMap, sync::Arc};

use crate::{
    frame::Frame,
    page::{Page, PageId},
};
use crossbeam_queue::ArrayQueue;
use tokio::sync::RwLock;

pub struct BufferPoolManager {
    pub(crate) frames: Vec<Frame>,
    pub(crate) free_frames: ArrayQueue<Frame>,
    pub(crate) pages: RwLock<HashMap<PageId, Arc<Page>>>,
}

impl BufferPoolManager {
    pub fn new(frame_num: usize) -> Self {
        // TODO create proper IoSlice frames
        Self {
            frames: (0..frame_num).map(|_| Frame::default()).collect(),
            free_frames: ArrayQueue::new(frame_num),
            pages: RwLock::new(HashMap::with_capacity(frame_num * 2)),
        }
    }
}
