use std::{collections::HashMap, sync::Arc};

use crate::messages::Informations;

pub struct Database {
    map: HashMap<String, Arc<Informations>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, infos: Informations) -> Option<Arc<Informations>> {
        self.map.insert(infos.service.clone(), Arc::new(infos))
    }

    pub fn get(&self, service: &str) -> Option<Arc<Informations>> {
        self.map.get(service).cloned()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<Arc<Informations>> {
        self.map.remove(name)
    }
}
