use crate::FrameId;

pub enum AccessType {
    Lookup,
    Scan,
    Index,
    Unknown,
}

pub trait Replacer {
    fn pin(&mut self, frame: FrameId);

    fn unpin(&mut self, frame: FrameId);

    fn record_access(&mut self, frame: FrameId, access: AccessType);

    fn evict(&mut self) -> FrameId;

    fn remove(&mut self, frame: FrameId);

    fn size(&self) -> usize;
}

pub mod clock;
