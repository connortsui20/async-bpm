use std::sync::atomic::AtomicBool;

pub type Temperature = AtomicBool;
pub const HOT: bool = true;
pub const COOL: bool = false;
