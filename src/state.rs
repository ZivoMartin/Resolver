use std::sync::Arc;

use notifier_hub::notifier::NotifierHub;
use tokio::sync::{Mutex, RwLock};

use crate::{database::Database, messages::Informations};

#[derive(Clone, Copy, Debug)]
pub enum TimerReset {
    Unchanged,
    Infinite,
    Set(u64),
}

type RegistrationHub = NotifierHub<Informations, String>;
type TimerResetHub = NotifierHub<TimerReset, String>;

#[derive(Clone)]
pub struct State {
    pub registration_hub: Arc<Mutex<RegistrationHub>>,
    pub timer_reset_hub: Arc<Mutex<TimerResetHub>>,
    pub database: Arc<RwLock<Database>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            registration_hub: Arc::new(Mutex::new(NotifierHub::new())),
            timer_reset_hub: Arc::new(Mutex::new(NotifierHub::new())),
            database: Arc::new(RwLock::new(Database::new())),
        }
    }
}
