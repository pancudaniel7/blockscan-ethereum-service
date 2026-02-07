# Blockscan-service

## Description

**Blockscan** is a production‑ready microservice that ingests Ethereum blocks and reliably
publishes them to Kafka. It supports both **low‑latency** streaming of **new head**s and
reorg‑safe ingestion of **finalized** blocks, enabling diverse downstream use cases. For now
it uses an Alchemy WebSocket endpoint to read new blocks (configurable). 

The service is **reliable** by design, it provides **effectively‑once** delivery with idempotent
publishing and deterministic per‑key ordering, tolerates node/broker outages with
bounded **retries** and **graceful recovery**, and **preserves** progress across **restarts**. 

It offers a simple fail‑over mechanism based on two replicas, and provides observability metrics,
logs, and health endpoints for operational clarity. Designed to be simple to integrate
and safe to operate, Blockscan service is a robust foundation for real‑time pipelines,
analytics, and indexing workloads.

## Architecture

Blockscan adopts Clean Architecture, a framework‑agnostic core (entities, ports, use cases),
adapters implementing those ports (Ethereum scanner, Redis store/stream, Kafka publisher),
and infrastructure that wires configuration, HTTP server, metrics, and lifecycle.
At runtime, the Scanner ingests blocks (new heads or finalized) and maps them to domain models;
a Redis outbox enforces dedup and durable handoff via Streams; a Stream Reader publishes to Kafka
and acks on success. Components are loosely coupled for testability, scalability, and operability.

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

In non-finalized mode, reorgs may appear in the stream; finalized mode avoids them.

### Diagram

<img src="docs/architecture.png" alt="Architecture – Ethereum Block → Kafka" width="900">
