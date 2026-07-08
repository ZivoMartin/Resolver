# Beacon

> A lightweight MQTT-based service discovery registry.

Beacon is a small service registry designed for distributed systems communicating over MQTT. Services announce themselves by publishing their endpoint information, periodically send heartbeats to remain alive, and clients can query Beacon to discover available services.

Unlike traditional DNS or heavyweight service discovery systems, Beacon is designed to be simple, asynchronous, and entirely message-driven.

## Features

* Dynamic service discovery
* Heartbeat-based liveness detection
* Automatic expiration using configurable TTLs
* Arbitrary JSON metadata attached to services
* Optional "wait until available" requests
* Fully asynchronous implementation using Tokio
* MQTT-native protocol

---

## Overview

Each service registers itself with Beacon by publishing a registration message.

```
Service ── register ──► Beacon
```

Beacon stores the service information in memory.

Services may periodically send heartbeat messages to refresh their expiration timer.

```
Service ── heartbeat ──► Beacon
```

Clients discover services by publishing a request.

```
Client ── request ──► Beacon
                     │
                     ▼
              reply topic
```

If requested, Beacon can wait until all requested services become available before replying.

---

## Registration

Publish to

```
<name>/register
```

Payload:

```json
{
  "service": "database",
  "host": "10.0.0.5",
  "port": 5432,
  "ttl_ms": 30000,
  "metadatas": {
    "region": "eu-west",
    "version": "1.0"
  }
}
```

Fields:

| Field       | Description                                                         |
| ----------- | ------------------------------------------------------------------- |
| `service`   | Unique service identifier                                           |
| `host`      | Hostname or IP                                                      |
| `port`      | Service port                                                        |
| `ttl_ms`    | Optional expiration timeout. If omitted, the service never expires. |
| `metadatas` | Arbitrary JSON metadata                                             |

Attempting to register the same service twice results in an error.

---

## Heartbeats

Publish to

```
<name>/heartbeat
```

Payload:

```json
{
  "service": "database"
}
```

A heartbeat refreshes the service's TTL.

If no heartbeat is received before the TTL expires, Beacon automatically removes the service.

---

## Updating a Service

Publish to

```
<name>/update
```

Payload is identical to a registration message.

Updating allows changing:

* host
* port
* metadata
* TTL

without unregistering first.

---

## Service Discovery

Publish to

```
<name>/request
```

Payload:

```json
{
  "services": [
    "database",
    "cache"
  ],
  "reply_topic": "client/reply",
  "retain": false
}
```

### Fields

| Field         | Description                                  |
| ------------- | -------------------------------------------- |
| `services`    | Requested service names                      |
| `reply_topic` | MQTT topic used for the response             |
| `retain`      | Wait until missing services become available |
| `timeout_ms`  | Optional timeout when `retain` is enabled    |

---

### Immediate lookup

When `retain` is `false`, Beacon immediately returns every service currently available.

Example response:

```json
{
  "infos": {
    "database": {
      "service": "database",
      "host": "10.0.0.5",
      "port": 5432,
      "ttl_ms": 30000,
      "metadatas": {}
    }
  }
}
```

---

### Waiting for services

If `retain` is `true`, Beacon waits until every requested service has registered.

This is useful during startup when services may appear asynchronously.

A timeout may be provided to avoid waiting indefinitely.

---

## Topics

For a Beacon instance named `beacon`, the following MQTT topics are used:

| Topic              | Purpose            |
| ------------------ | ------------------ |
| `beacon/register`  | Register a service |
| `beacon/update`    | Update a service   |
| `beacon/heartbeat` | Refresh TTL        |
| `beacon/request`   | Query services     |

Responses are published to the client-provided reply topic.

---

## Running

```bash
cargo run -- \
    --name beacon \
    --broker localhost \
    --broker-port 1883
```

Available options:

| Option          | Default     |
| --------------- | ----------- |
| `--name`        | `resolver`  |
| `--broker`      | `mosquitto` |
| `--broker-port` | `1883`      |
| `--channel-cap` | `100`       |

---

## Design

Beacon keeps an in-memory registry of active services.

Each registered service owns an independent expiration timer. Whenever a heartbeat or update is received, the timer is refreshed. If the timer expires, the service is automatically removed from the registry.

Service discovery requests never block unless the client explicitly enables `retain`, in which case Beacon waits until the requested services become available or a timeout is reached.

Internally, Beacon uses Tokio tasks and asynchronous notification channels to efficiently coordinate registrations, timer updates, and pending discovery requests.

---

## License

MIT
