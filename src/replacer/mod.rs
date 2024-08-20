use crate::page::PageId;

pub enum AccessType {
    Lookup,
    Scan,
    Index,
    Unknown,
}

#[derive(Debug)]
pub struct FrameNotFound;

pub trait Replacer {
    fn new(num_frames: usize) -> Self;

    fn pin(&self, pid: PageId) -> Result<usize, FrameNotFound>;

    fn unpin(&self, pid: PageId) -> Result<usize, FrameNotFound>;

    fn record_access(&self, pid: PageId, access: AccessType) -> Result<(), FrameNotFound>;

    // Adds a page into the replacer.
    fn add(&self, pid: PageId);

    // Finds a page to evict. Returns None if all pids are pinned.
    fn evict(&self) -> Option<PageId>;

    // Force evict a pid. TODO
    fn remove(&self, pid: PageId) -> Result<(), FrameNotFound>;

    fn size(&self) -> usize;
}

pub mod clock;
