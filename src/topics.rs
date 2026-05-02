pub fn register(name: &str) -> String {
    format!("{name}/register")
}

pub fn update(name: &str) -> String {
    format!("{name}/update")
}

pub fn heartbeat(name: &str) -> String {
    format!("{name}/heartbeat")
}

pub fn request(name: &str) -> String {
    format!("{name}/request")
}
