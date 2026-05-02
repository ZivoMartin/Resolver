use anyhow::{Context, Result, bail};
use notifier_hub::notifier::ChannelState;
use rumqttc::{AsyncClient, QoS};
use std::{
    collections::{HashMap, HashSet},
    future::pending,
    pin::Pin,
};
use tokio::{
    select,
    time::{Duration, sleep},
};
use tracing::{debug, info, warn};

use crate::{
    messages::{Heartbeat, Informations, Reply, Request},
    state::{State, TimerReset},
};

async fn reset_timer(state: State, service: String, timer_update: TimerReset) -> Result<()> {
    let hub = state.timer_reset_hub.lock().await;
    hub.clone_send(timer_update, &service).with_context(|| {
        format!("Timer reset failed: no channel initialized for service '{service}'")
    })?;
    debug!(service = %service, update = ?timer_update, "Timer reset signal sent");
    Ok(())
}

pub async fn handle_heartbeat(state: State, s: &str) -> Result<()> {
    let heartbeat = serde_json::from_str::<Heartbeat>(&s)
        .context("Failed to parse heartbeat message (invalid JSON or schema)")?;

    debug!(service = %heartbeat.service, "Heartbeat received");
    reset_timer(state, heartbeat.service, TimerReset::Unchanged).await
}

pub async fn handle_update(state: State, s: &str) -> Result<()> {
    let infos = serde_json::from_str::<Informations>(&s)
        .context("Failed to parse update message (Informations)")?;

    {
        let mut database = state.database.write().await;
        if database.contains(&infos.service) {
            database.insert(infos.clone());
            info!(service = %infos.service, "Service updated");
        } else {
            bail!(
                "Update rejected: service '{}' is not registered",
                infos.service
            );
        }
    }

    let timer_update = if let Some(ttl) = infos.ttl_ms {
        TimerReset::Set(ttl)
    } else {
        TimerReset::Infinite
    };

    reset_timer(state, infos.service, timer_update).await
}

async fn start_timer(state: State, service: String, mut dur: Duration) {
    let mut reset_receiver = {
        let mut hub = state.timer_reset_hub.lock().await;
        hub.subscribe(&service, 10)
    };

    debug!(service = %service, duration_ms = ?dur.as_millis(), "Timer started");

    loop {
        select! {
            _ = sleep(dur) => {
                // TTL expired -> remove service
                let mut database = state.database.write().await;
                database.remove(&service);
                info!(service = %service, "Service expired and removed from database");
                break
            },
            timer_update = reset_receiver.recv() => {
                let Some(timer_update) = timer_update else {
                    debug!(service = %service, "Timer channel closed");
                    break
                };
                match timer_update {
                    TimerReset::Unchanged => continue,
                    TimerReset::Infinite => {
                        dur = Duration::MAX;
                        debug!(service = %service, "Timer set to infinite");
                    }
                    TimerReset::Set(x) => {
                        dur = Duration::from_millis(x);
                        debug!(service = %service, duration_ms = x, "Timer updated");
                    }
                }
            }
        }
    }
}

pub async fn handle_register(state: State, s: &str) -> Result<()> {
    let infos = serde_json::from_str::<Informations>(&s)
        .context("Failed to parse registration message (Informations)")?;
    let id = infos.service.clone();

    {
        let mut database = state.database.write().await;

        if database.contains(&infos.service) {
            bail!(
                "Registration rejected: service '{}' already exists",
                infos.service
            )
        }

        info!(service = %infos.service, ttl = ?infos.ttl_ms, "Registering service");
        database.insert(infos.clone());
    }

    let dur = if let Some(ttl) = infos.ttl_ms {
        Duration::from_millis(ttl)
    } else {
        Duration::MAX
    };

    // Spawn TTL watchdog for the service
    let state_cloned = state.clone();
    let service = infos.service.clone();
    tokio::spawn(async move { start_timer(state_cloned, service, dur).await });

    let hub = state.registration_hub.lock().await;

    if matches!(hub.channel_state(&id), ChannelState::Running) {
        hub.clone_send(infos, &id)
            .context("Failed to broadcast registration notification to subscribers")?;
        debug!(service = %id, "Registration notification broadcasted");
    }

    Ok(())
}

async fn build_reply(state: State, services: &[String]) -> Reply {
    let database = state.database.read().await;
    let infos = services
        .iter()
        .filter_map(|service| {
            let infos = database.get(&service).map(|info| info.as_ref().to_owned());
            if infos.is_none() {
                warn!(service = %service, "Requested service not found in database");
            }
            infos.map(|infos| (service.to_owned(), infos.to_owned()))
        })
        .collect::<HashMap<_, _>>();
    Reply { infos }
}

async fn wait_for_batch(state: State, services: &[String], timeout_ms: Option<u64>) -> Reply {
    let mut receiver = {
        let mut hub = state.registration_hub.lock().await;
        let ids = services.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        let receiver = hub.subscribe_multiple(&ids, services.len());

        receiver
    };

    let (mut missing, mut infos_map) = {
        let database = state.database.read().await;
        let mut missing = HashSet::with_capacity(services.len());
        let mut infos_map = HashMap::with_capacity(services.len());

        for service in services {
            match database.get(&service) {
                Some(infos) => {
                    infos_map.insert(service.to_owned(), infos.as_ref().to_owned());
                }
                None => {
                    missing.insert(service);
                }
            }
        }
        (missing, infos_map)
    };

    let mut timer: Pin<Box<dyn Future<Output = ()> + Send>> = match timeout_ms {
        Some(dur) => Box::pin(sleep(Duration::from_millis(dur))),
        None => Box::pin(pending::<()>()),
    };

    while !missing.is_empty() {
        select! {
            _ = &mut timer => {
                warn!(
                    remaining = missing.len(),
                    "Timeout while waiting for services to become available"
                );
                break
            },
            infos = receiver.recv() => {
                let Some(infos) = infos else {
                    debug!("Registration channel closed while waiting for batch");
                    break
                };
                missing.remove(&infos.service);
                infos_map.insert(infos.service.clone(), infos);
            }
        }
    }

    Reply { infos: infos_map }
}

pub async fn handle_request(state: State, client: AsyncClient, request_str: &str) -> Result<()> {
    let request = serde_json::from_str::<Request>(request_str)
        .context("Failed to parse request message (Request)")?;

    debug!(
        services = ?request.services,
        retain = request.retain,
        timeout_ms = ?request.timeout_ms,
        "Handling request"
    );

    let reply = if request.retain {
        wait_for_batch(state, &request.services, request.timeout_ms).await
    } else {
        build_reply(state, &request.services).await
    };

    let s = if reply.infos.len() == 1 {
        serde_json::to_string(&reply.infos.into_values().next())
            .context("Failed to serialize reply to JSON")?
    } else {
        serde_json::to_string(&reply).context("Failed to serialize reply to JSON")?
    };

    client
        .publish(
            request.reply_topic.clone(),
            QoS::AtLeastOnce,
            false,
            s.as_bytes(),
        )
        .await
        .with_context(|| format!("Failed to publish reply to topic '{}'", request.reply_topic))?;

    debug!(topic = %request.reply_topic, "Reply published");

    Ok(())
}
