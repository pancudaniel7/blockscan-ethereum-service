# Blockscan-service

## Description

A **production-grade** microservice that ingests new Ethereum **blocks** in real time and publishes them to Kafka as canonical block events. 
It is built for horizontal **scalability** and fault tolerance, using efficient WebSocket polling or subscription clients, partition-aware load balancing, 
and stateless replicas behind a lightweight coordinator. 

It is engineered for **reliability** through exactly-once publishing through idempotent Kafka producers, transactional writes, and deterministic deduplication keys per block hash and height. 
Temporary outages are handled with durable buffering and retry policies with exponential backoff, so events are never dropped and order is preserved within a partition.

The service is **reorg-aware**, emitting update or tombstone events for replaced blocks to keep downstream consumers consistent. Comprehensive observability is included with 
structured logs, metrics, and traces to monitor lag, throughput, error rates, and reorg frequency.

## Architecture

Replicas subscribe via WebSocket to an Ethereum node and publish new blocks to Kafka in real time.
Redis handles coordination using dedup keys per block hash with a TTL also Redis outbox stores pending events 
and is drained on restart for crash safety.

**Exactly-once** delivery uses idempotent and transactional Kafka producers with deterministic keys.
Reorgs are detected and the service emits update or tombstone events to keep downstream state correct.

### Diagram

<img src="docs/architecture.png" alt="Architecture – Ethereum Block → Kafka" width="900">



