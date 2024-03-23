use crate::frame::Frame;
use crossbeam_queue::ArrayQueue;

pub struct BufferPoolManager {
    pub(crate) frames: Vec<Frame>,
    pub(crate) free_frames: ArrayQueue<Frame>,
}

impl BufferPoolManager {
    pub fn new(frame_num: usize) -> Self {
        // TODO create proper IoSlice frames
        Self {
            frames: (0..frame_num).map(|_| Frame::default()).collect(),
            free_frames: ArrayQueue::new(frame_num),
        }
    }
}
