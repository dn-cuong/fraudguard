# Architecture

## Path

1. Client POSTs a payment (API Gateway in AWS, `cmd/ingest` locally).
2. Record lands on Kinesis, partition key = `card_id`.
3. EC2 workers pull shards and fan work across a goroutine pool + channel.
4. Each score:
   - Redis velocity (`vel:card:*`, `vel:ip:*`)
   - DynamoDB dispute / history lookup
   - pure rule eval (`AMOUNT_HIGH`, `VELOCITY_*`, `HISTORY_DISPUTE`)
   - incr Redis TTLs + write scored txn to DynamoDB
5. Workers expose `/metrics`; CloudWatch alarms on missing heartbeat.

## Why two stores

- Redis: hot windows under burst (sub-ms).
- DynamoDB: durable history / audit.

Workers stay stateless; shared state keeps replicas consistent.

## Latency budget

Rough path for p50 &lt; 80ms: Redis round-trips + DynamoDB query/put + rule eval.
`cmd/loadgen` reports p50/p99 against the ingest API.
