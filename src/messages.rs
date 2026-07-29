use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Informations {
    pub service: String,
    pub host: String,
    pub port: u16,
    pub metadatas: Value,
    pub ttl_ms: Option<u64>,
    pub overwrite: Option<bool>,
}

fn default_false() -> bool {
    false
}

#[derive(Deserialize)]
pub struct Heartbeat {
    pub service: String,
}

#[derive(Deserialize)]
pub struct Request {
    pub services: Vec<String>,
    pub reply_topic: String,
    #[serde(default = "default_false")]
    pub retain: bool,
    pub timeout_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct Reply {
    pub infos: HashMap<String, Informations>,
}
