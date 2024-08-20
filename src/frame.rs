use std::ops::{Deref, DerefMut};

pub struct Frame {
    frame_id: usize,
    buf: &'static mut [u8],
}

impl Frame {
    pub fn new(frame_id: usize, buf: &'static mut [u8]) -> Self {
        Self { frame_id, buf }
    }

    pub fn id(&self) -> usize {
        self.frame_id
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl DerefMut for Frame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}
