# Blockscan-service

## Description

A **production-grade** microservice that ingests new Ethereum **blocks** in real time and publishes them to Kafka as canonical block events. 
It is built for horizontal **scalability** and fault tolerance, using efficient WebSocket polling or subscription clients, partition-aware load balancing, 
and stateless replicas behind a lightweight coordinator. 

It is engineered for **reliability** through effectively-once publishing via transactional Kafka producers, deterministic keys, and durable Redis markers. Downstream consumers can idempotently apply updates using the block hash key and the source message id header. 
Temporary outages are handled with durable buffering and retry policies with exponential backoff, so events are never dropped and order is preserved within a partition.

The service is **reorg-aware**, emitting update or tombstone events for replaced blocks to keep downstream consumers consistent. Comprehensive observability is included with 
structured logs, metrics, and traces to monitor lag, throughput, error rates, and reorg frequency.

## Architecture

Replicas subscribe via WebSocket to an Ethereum node and publish new blocks to Kafka in real time.
Redis handles coordination using dedup keys per block hash with a TTL also Redis outbox stores pending events 
and is drained on restart for crash safety.

**Delivery semantics** are effectively-once: Redis ensures one stream entry per block hash (within TTL), the publisher uses transactional producers and deterministic keys, and a durable "published" marker prevents re-processing on restart. A rare duplicate can occur in a narrow crash window; consumers can dedupe using the `source-message-id` header and/or rely on log compaction.

## Event Format

- Topic: configurable via `kafka.topic` (see `configs/*.yml`)
- Key: block hash bytes (keeps per-hash ordering and compaction efficiency)
- Value: JSON-encoded block (see `internal/core/usecase/mapper.go` for mapping)
- Headers:
  - `block-number`: decimal string of the block height
  - `block-hash`: hex string (0x-prefixed)
  - `source-message-id`: Redis stream message ID that produced this record (enables downstream dedupe)

Example downstream dedupe options:
- Upsert by block hash key (idempotent writes)
- Keep a small seen-set keyed by `source-message-id`
- Use a compacted Kafka topic so only the latest per block hash persists

## Compacted Topic

The local provisioner now creates the upstream topic with log compaction enabled (`cleanup.policy=compact`). See `deployments/kafka/topics.sh` and the `kafka-provisioner` service in `deployments/docker-compose.yml`.

- New topic creation uses `KAFKA_TOPIC_CLEANUP_POLICY` (defaults to `compact`).
- If your topic already exists, alter it manually:
  - `kafka-configs.sh --bootstrap-server <broker> --alter --topic <topic> --add-config cleanup.policy=compact`

Reorgs are detected and the service emits update or tombstone events to keep downstream state correct.

### Diagram

<img src="docs/architecture.png" alt="Architecture – Ethereum Block → Kafka" width="900">


