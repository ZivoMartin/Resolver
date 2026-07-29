use anyhow::Result;
use clap::Parser;
use rumqttc::MqttOptions;
use tracing::info;

use crate::listen::listen;

mod database;
mod handler;
mod listen;
mod messages;
mod state;
mod topics;

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(long, default_value_t = String::from("resolver"))]
    name: String,
    #[arg(long, default_value_t = String::from("mosquitto"))]
    broker: String,
    #[arg(long, default_value_t = 1883)]
    broker_port: u16,
    #[arg(long, default_value_t = 100)]
    channel_cap: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    info!("Starting Resolver.");

    let Args {
        name,
        broker,
        broker_port,
        channel_cap,
    } = Args::parse();

    let options = MqttOptions::new(&name, broker, broker_port);
    listen(options, &name, channel_cap).await
}
