use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS};
use tracing::{error, info, warn};

use crate::{
    handler::{handle_heartbeat, handle_register, handle_request, handle_update},
    state::State,
    topics,
};

pub async fn listen(options: MqttOptions, name: &str, channel_cap: usize) -> Result<()> {
    let (client, mut eventloop) = AsyncClient::new(options, channel_cap);

    let request_topic = topics::request(name);
    let register_topic = topics::register(name);
    let update_topic = topics::update(name);
    let heartbeat_topic = topics::heartbeat(name);
    let topics = [&register_topic, &request_topic, &update_topic];

    for topic in topics {
        client
            .subscribe(topic, QoS::AtLeastOnce)
            .await
            .with_context(|| format!("MQTT subscription failed for topic '{topic}'"))?;
    }

    let state = State::new();

    info!(topics = ?topics, "MQTT listener initialized");

    loop {
        match eventloop
            .poll()
            .await
            .context("MQTT event loop polling failed")?
        {
            Event::Incoming(Packet::Publish(Publish { payload, topic, .. })) => {
                // Payloads are expected to be valid UTF-8 JSON messages
                let Ok(s) = String::from_utf8(payload.to_vec()) else {
                    warn!(topic = %topic, "Received non-UTF8 payload, ignoring message");
                    continue;
                };

                info!(topic = %topic, "Received message");

                let state = state.clone();
                let client = client.clone();

                if topic == register_topic {
                    tokio::spawn(async move {
                        if let Err(e) = handle_register(state, &s).await {
                            error!(error = %e, "Failed to handle register message");
                        }
                    });
                } else if topic == update_topic {
                    if let Err(e) = handle_update(state, &s).await {
                        error!(error = %e, "Failed to handle update message");
                    }
                } else if topic == heartbeat_topic {
                    if let Err(e) = handle_heartbeat(state, &s).await {
                        error!(error = %e, "Failed to handle heartbeat message");
                    }
                } else if topic == request_topic {
                    tokio::spawn(async move {
                        if let Err(e) = handle_request(state, client.clone(), &s).await {
                            error!(error = %e, "Failed to handle request message");
                        }
                    });
                } else {
                    // This should not happen unless subscriptions or routing are misconfigured
                    warn!(topic = %topic, "Received message on unexpected topic");
                }
            }
            _ => continue,
        }
    }
}
