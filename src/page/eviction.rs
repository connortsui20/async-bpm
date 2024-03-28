use std::sync::atomic::AtomicU8;

pub type Temperature = AtomicU8;

pub const HOT: u8 = 2;
pub const COOL: u8 = 1;
pub const COLD: u8 = 0;
