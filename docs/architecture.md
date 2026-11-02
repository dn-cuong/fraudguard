# Architecture

How FraudGuard scores a payment, why it's built this way, and where it falls short. For setup, see the [README](../README.md).

## Why stateless

A single scorer can't take bursts, and if each replica counts velocity in its own memory they stop agreeing with each other. So the workers keep nothing locally. Counters are in Redis, history is in DynamoDB, and the rules only look at `(payment, snapshot)`. Any worker with the same snapshot gives the same decision.

The Kinesis partition key is `card_id`, so one card always lands on the same shard. The pool scores a batch concurrently, so txns can be scored slightly out of order; the velocity counts allow for that (see [Velocity counting](#velocity-counting)). The downside is that a very busy card sits on one shard. That hasn't been tried.

## Data path

1. **Client → API Gateway** over HTTPS (`POST /payments`). A request model rejects malformed bodies with 400.
2. **API Gateway → Kinesis** (`PutRecord`), partitioned by `card_id`. If the put fails the client gets 502, not `queued`.
3. **Go workers** read shards and score through a goroutine pool.
4. **Shared state before deciding:**
   - Redis: velocity windows (`win:card:*`, `win:ip:*`)
   - DynamoDB: txn history and the dispute lookup
5. **Rule engine** produces `ALLOW`, `REVIEW` or `DECLINE`, and the txn record is written.
6. **Client polls** `GET /payments/{txn_id}`: `pending` while scoring, `failed` with the reason, or `scored` with `decision` and `score`.
7. **CloudWatch**: `WorkerHeartbeat` per worker, and `DeadLetters` across all of them.

`POST /payments` only means the event is on the stream. The local ingest binary serves the same flow under `/v1/payments`, and its `-mode score` scores inline for quick demos.

## Storage

| Store | Role |
|-------|------|
| **ElastiCache Redis** | Hot velocity windows with TTL (sub-ms reads under burst) |
| **DynamoDB** `transactions` | Durable txn history and dispute flag (system of record) |
| **DynamoDB** `leases` | Shard ownership and checkpoints |
| **DynamoDB** `dead-letters` | Records that couldn't be scored, with the raw bytes and the error |

Scored txns expire after 90 days through DynamoDB TTL (`expires_at`). Disputed txns drop their TTL and are kept, so `HISTORY_DISPUTE` doesn't forget a card. TTL deletes are lazy, usually within a couple of days of expiry.

**Key design.** The transactions table is keyed `card_id` + `txn_id`, and polling goes through the `txn_id-index` GSI. Keying on `txn_id` alone would give strongly consistent polls and drop one GSI write, but marking a dispute needs the card as well, and per-card history (the base for any rule beyond velocity) would need its own GSI. So the key stays, at the cost of one extra index write per txn and polls that can lag the write by a moment.

Disputes are found through `dispute-index`, a sparse GSI: only disputed txns carry `dispute_card_id`, so the lookup never scans a card's history.

## Shards, leases and checkpoints

Shards are shared between workers through leases in DynamoDB (`internal/lease`). A worker takes its fair share of shards and renews its leases every 5s. If it dies, the leases expire after 30s and another worker picks the shards up from the last checkpoint. There's no worker count to keep in sync anywhere, so hosts can be added or removed freely. On SIGTERM a worker hands its shards back straight away.

- A batch is checkpointed only after every record in it has a result. A crash replays the batch; scoring is idempotent (Redis counts by `txn_id`, and DynamoDB `Put` ignores a txn that's already stored), so replays are safe.
- Workers start from the oldest record when a shard has no checkpoint, so payments that arrived before any worker was up still get scored.
- After a reshard, a child shard starts only once its parent is fully read, so a card's newer events never get ahead of its older ones.

## Failure handling

- A failing score is retried 3 times with backoff. Invalid payments (bad IP, non-USD, oversized fields) aren't retried.
- If it still fails, an `ERROR` record is stored, so the client sees `failed`, and the raw record goes to the dead-letter table. Records that can't be parsed at all go there too.
- If either write fails, the batch isn't checkpointed and is read again, so nothing is lost silently.
- Dead letters are kept 14 days, keyed by shard and sequence number, with the error. The `fraudguard-dead-letters` alarm fires on any. Inspect them with `aws dynamodb scan --table-name fraudguard-dead-letters`.

## Velocity counting

Each txn is counted in Redis with one atomic Lua call on a sorted set of txn_ids, and the count it gets back is what the rules see. So concurrent payments on one card each get a different count, and a redelivered record isn't counted twice.

Windows run on arrival time (Kinesis arrival, or the ingest clock in score mode), not the time of scoring, so draining a backlog after an outage doesn't look like a burst. Client-supplied timestamps are ignored: they would let a caller dodge the velocity rules. Txns up to 1s apart still count each other when scored out of order. Payments without an `ip` skip `VELOCITY_IP`.

## Failover, checked locally

Against LocalStack Kinesis and DynamoDB Local, with `bin/ingest -mode kinesis` and two `bin/worker` processes:

- 20 payments sent while no worker was running were all scored once a worker started.
- `kill -9` on one worker: its shard sat `pending` until the lease expired, then the other worker resumed from the checkpoint and all payments were scored.
- Resharding 2 → 4 with traffic running: parents were read to the end first, then the 4 children; all payments were scored.
- Redis stopped: payments came back `failed` with the error, and scoring worked again once Redis was back.
- SIGTERM: the worker handed its shards back, so the next worker took them right away instead of waiting for the lease.

These were run before the arrival-time velocity and dead-letter changes and haven't been repeated since.

## Known limitations

- Not run on real AWS yet: only `terraform validate` and the local runs above.
- Lease expiry uses each worker's own clock, so hosts need roughly synced time (chrony on the AL2023 AMI does this). The 30s lease is far above normal skew.
- Deliveries are at-least-once. A replayed txn is counted at its original arrival time, so it can't inflate the current window.
- A backlog that drains slower than real time can undercount: a card's Redis key expires on wall-clock time, so it can vanish between two of that card's txns.
- The sliding window keeps one Redis entry per txn in the window, so a card or IP sending thousands of txns a minute costs memory until they age out.
- If the DynamoDB write of a scored txn fails, the txn is retried and finally stored as `ERROR`, but its Redis counts stay.
- USD only. Anything else is rejected; there's no FX.
- Dispute lookup goes through a GSI, so a dispute shows up a moment after it's marked.
- The AWS API has no auth, and the dispute endpoint exists only on the local ingest binary, so `HISTORY_DISPUTE` can't fire on AWS yet.
- SSH ingress is open by default; see [Deploying to AWS](aws.md#safety).
- The heartbeat alarms have no `alarm_actions`: they change state but notify and replace nothing.

## Code layout

| Path | Role |
|------|------|
| `cmd/ingest` | HTTP ingest (score mode or Kinesis put), status poll, dispute |
| `cmd/worker` | Kinesis consumer with a goroutine pool |
| `cmd/loadgen` | TPS / p50 / p99 harness |
| `internal/engine` | Pure rules over `(payment, snapshot)` |
| `internal/scorer` | Reads shared state, calls the engine, writes the result |
| `internal/velocity` | Redis sliding windows |
| `internal/history` | DynamoDB txn store |
| `internal/stream` | Kinesis producer and lease-based consumer |
| `internal/lease` | Shard leases and checkpoints |
| `internal/deadletter` | Records that couldn't be scored |
| `internal/cloudwatchx`, `internal/heartbeat` | Metrics |
| `config/` | `local.yaml`, `aws.yaml`, `compose.yaml` |
| `terraform/` | VPC, API Gateway, Kinesis, ElastiCache, DynamoDB, EC2, alarms |
| `ansible/` | Worker binary and systemd unit |
