use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug)]
pub struct Temperature {
    inner: AtomicU8,
}

#[derive(Debug)]
#[repr(u8)]
pub enum TemperatureState {
    Hot = 2,
    Cool = 1,
    Cold = 0,
}

impl Temperature {
    pub fn new(state: TemperatureState) -> Self {
        Self {
            inner: AtomicU8::new(state as u8),
        }
    }

    pub fn store(&self, state: TemperatureState, order: Ordering) {
        self.inner.store(state as u8, order);
    }

    pub fn load(&self, order: Ordering) -> TemperatureState {
        match self.inner.load(order) {
            0 => TemperatureState::Cold,
            1 => TemperatureState::Cool,
            2 => TemperatureState::Hot,
            _ => unreachable!("Had an invalid value inside the `Temperature` struct"),
        }
    }
}
