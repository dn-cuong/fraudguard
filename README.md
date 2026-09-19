# FraudGuard

![FraudGuard system architecture](images/fraudguard_architecture.png)

Distributed real-time fraud scoring pipeline on AWS. Payments enter through API Gateway into Kinesis; Go workers on EC2 apply shared rule checks using ElastiCache (Redis) velocity counters and DynamoDB transaction history, then expose the decision over `GET /payments/{txn_id}`.

Decisions are `ALLOW`, `REVIEW` or `DECLINE`. Terraform for infra, Ansible to deploy the workers, CloudWatch alarms on worker heartbeats.

Side project to see what happens when a few scorer replicas share state. Just rules, no ML.

---

## Why stateless

A single scorer can't take bursts, and if each replica counts velocity in its own memory they stop agreeing with each other. So the workers keep nothing locally. Counters are in Redis, history in DynamoDB, and the rules only look at `(payment, snapshot)`.

Kinesis partition key is `card_id`, so one card always lands on the same shard and stays in order. Downside: a very busy card sits on one shard. Haven't tried it.

---

## Architecture

### Data path

1. **Client → API Gateway** over HTTPS (`POST /payments`).
2. **API Gateway → Kinesis** (`PutRecord`). Same `card_id` stays on one partition for ordering; different cards fan out across shards.
3. **EC2 Go workers** poll shards and score through a goroutine pool + channel.
4. **Shared state before decide**
   - Redis / ElastiCache: velocity windows (`vel:card:*`, `vel:ip:*`)
   - DynamoDB: durable history / dispute lookup
5. **Rule engine** produces ALLOW / REVIEW / DECLINE and writes the txn record.
6. **Client polls** `GET /payments/{txn_id}`
   - `pending` while scoring
   - `failed` if it couldn't be scored (with the reason)
   - `scored` with `decision` + `score`
7. **CloudWatch** `WorkerHeartbeat` metrics; missing heartbeats trip failover alarms.

### Async ingest

`POST /payments` returns quickly:

```json
{"status":"queued","txn_id":"...","poll":"/payments/..."}
```

That only means the event is on the stream. The decision comes from `GET /payments/{txn_id}` (DynamoDB GSI on `txn_id`). Local **score mode** can still return the decision inline for faster demos; polling works locally at `GET /v1/payments/{txn_id}` as well.

> Note: the AWS/API Gateway deployment exposes `/payments` and `/payments/{txn_id}`, while the local ingest binary exposes the same flow under `/v1/payments` and `/v1/payments/{txn_id}` for convenience. The architecture diagram reflects the AWS path, and the local endpoints are a thin wrapper around the same pipeline.

### Why Redis + DynamoDB

| Store | Role |
|-------|------|
| **ElastiCache Redis** | Hot velocity counters with TTL (sub-ms reads under burst) |
| **DynamoDB** | Durable txn history + dispute flag (system of record) |

Replicas stay consistent because scoring depends on shared stores, not process-local RAM.

Shards are shared between workers through leases in a small DynamoDB table (`internal/lease`). A worker takes its fair share of shards, renews the lease every few seconds, and if it dies the lease expires (30s) and another worker picks the shard up from the last checkpoint. There's no worker count to keep in sync anywhere, so you can add or remove hosts freely.

- A batch is checkpointed only after every record in it has a result. A crash replays the batch; scoring is idempotent (Redis counts by `txn_id`, DynamoDB `Put` ignores a txn that's already stored), so replays are safe.
- Workers start from the oldest record when a shard has no checkpoint, so payments that arrived before any worker was up still get scored.
- After a reshard, a child shard starts only once its parent is fully read, so a card's events stay in order.
- A failing score is retried 3 times. If it still fails, an `ERROR` record is stored and the client sees `failed`. If even that can't be stored, the batch isn't checkpointed and gets read again.

---

## Rules (v1)

| Rule | Signal | Store |
|------|--------|-------|
| `AMOUNT_HIGH` | amount ≥ threshold (default $2500) | config |
| `VELOCITY_CARD` | more than N txns / card / window | Redis |
| `VELOCITY_IP` | more than N txns / IP / window | Redis |
| `HISTORY_DISPUTE` | prior dispute on card | DynamoDB |

Mark a scored txn as disputed (local ingest) so later payments on that card trip `HISTORY_DISPUTE`:

```bash
curl -s -X POST http://localhost:8080/v1/payments/TXN_ID/dispute \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-1"}'
```

Each txn is counted in Redis with one atomic Lua call (INCR + TTL + a per-txn marker), and the count it gets back is what the rules see. So concurrent payments on one card each get a different count, and a redelivered record isn't counted twice.

Score stack: ≥80 `DECLINE`, ≥40 `REVIEW`, else `ALLOW`. Rules are pure over `(payment, snapshot)` so any worker with the same snapshot scores the same way.

---

## Layout

| Path | Role |
|------|------|
| `cmd/ingest` | HTTP ingest (score mode or Kinesis put) + status poll |
| `cmd/worker` | Kinesis consumer (goroutine pool) |
| `cmd/loadgen` | TPS / p50 / p99 harness |
| `internal/` | engine, velocity, history, stream, scorer, cloudwatch |
| `config/` | `local.yaml`, `aws.yaml` |
| `terraform/` | VPC, API Gateway, Kinesis, ElastiCache, DynamoDB, EC2, alarms |
| `ansible/` | worker binary + systemd |
| `images/` | architecture diagram |
| `scripts/` | smoke / LocalStack helpers |

---

## Local

Needs Go 1.25+ and Docker Desktop.

```bash
docker compose up -d redis dynamodb
make build
./bin/ingest -config config/local.yaml -mode score
```

Normal payment (`ALLOW`):

```bash
curl -s -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-1","user_id":"u1","amount":42,"merchant":"Cafe","country":"US","ip":"1.2.3.4"}'
```

High amount (`REVIEW` / `AMOUNT_HIGH`):

```bash
curl -s -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-hot","user_id":"u2","amount":3000,"merchant":"Luxury","country":"US","ip":"9.9.9.9"}'
```

Poll by `txn_id`:

```bash
curl -s http://localhost:8080/v1/payments/TXN_ID
```

Loadgen (1k TPS):

```bash
./bin/loadgen -addr http://localhost:8080 -tps 1000 -duration 15s -workers 64
```

```bash
make test   # engine, scorer, velocity, consumer, config
# lease and history tests need DynamoDB Local, otherwise they skip:
DYNAMO_TEST_ENDPOINT=http://localhost:8000 go test ./...
make up     # redis + dynamodb-local
make smoke  # POST payment then poll until scored
make down
```

### Benchmarks

Local only: `-mode score` (inline scoring), redis and dynamodb-local in Docker on a Mac (Apple M3 Pro). `./bin/loadgen -tps 1000 -duration 15s -workers 64`:

| target TPS | achieved | errors | p50 | p99 |
|-----------|----------|--------|-----|-----|
| 1000 | 999 | 0 | 1.3 ms | 31 ms |

One run, and it doesn't include Kinesis or API Gateway, so it says nothing about AWS latency.

### Failover, checked locally

Against LocalStack Kinesis + DynamoDB Local, with `bin/ingest -mode kinesis` and two `bin/worker` processes:

- 20 payments sent while no worker was running were all scored once a worker started.
- `kill -9` on one worker: its shard sat `pending` until the lease expired, then the other worker resumed from the checkpoint and all payments were scored.
- Resharding 2 → 4 with traffic running: parents were read to the end first, then the 4 children; all payments scored.
- Redis stopped: payments came back `failed` with the error, and scoring worked again once Redis was back.
- SIGTERM: the worker hands its shards back, so the next worker takes them right away instead of waiting for the lease.

-----------|----------|--------|-----|-----|
| 1000 | 996 | 0 | 1.3 ms | 81 ms |

That's one run, and it doesn't include Kinesis or API Gateway, so it says nothing about the AWS latency.

---

## Known limitations

- Not tested on real AWS yet, only Terraform validate plus the LocalStack run above.
- Lease expiry uses each worker's own clock, so hosts need roughly synced time (chrony on the AL2023 AMI does this). The 30s lease is far above normal skew.
- Deliveries are at-least-once. A txn that's replayed after its Redis entry has left the window gets counted again, which only matters if a worker is down for longer than the window.
- The sliding window keeps one Redis entry per txn in the window, so a card or IP sending thousands of txns a minute costs memory until they age out.
- If the DynamoDB write of a scored txn fails, the txn is retried and finally stored as `ERROR`, but its Redis counts stay.
- USD only. Anything else is rejected, there's no FX.
- Dispute lookup goes through a GSI, so a dispute shows up a moment after it's marked. Txns disputed before the `dispute-index` existed aren't in it.
- SSH ingress is open by default, see Safety.

---

## AWS

Provisions API Gateway, Kinesis, ElastiCache, DynamoDB (transactions with `txn_id-index` and `dispute-index`, plus the lease table), EC2 workers, bastion, CloudWatch.

**Cost:** NAT + ElastiCache dominate. Apply → smoke → `terraform destroy` in one sitting for demos.

### 1. Tooling

```bash
brew install awscli terraform ansible
aws configure --profile fraudguard
export AWS_PROFILE=fraudguard
aws sts get-caller-identity
```

### 2. Infra

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set key_name
terraform init && terraform apply
terraform output payments_url
terraform output payment_status_url_template
```

### 3. Workers

```bash
GOOS=linux GOARCH=amd64 go build -o bin/worker ./cmd/worker
cd ansible
cp inventory.example.yml inventory.yml         # from terraform outputs
ansible-playbook -i inventory.yml site.yml --private-key ~/.ssh/fraudguard.pem
```

### 4. Client flow

```bash
BASE="$(cd terraform && terraform output -raw payments_url)"

curl -s -X POST "$BASE" \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-1","user_id":"u1","amount":42,"merchant":"Cafe","country":"US","ip":"1.2.3.4"}'
# {"status":"queued","txn_id":"...","poll":"/payments/..."}

curl -s "$BASE/TXN_ID"
# {"status":"scored","decision":"ALLOW","score":0,...}
```

### 5. Teardown

```bash
cd terraform && terraform destroy
```

---

## Safety

- Do not commit `.env`, real `*.tfvars`, access-key CSVs, or PEM keys
- Set `ssh_ingress_cidr` in `terraform.tfvars` to your IP/32 before apply (default is open for demos)
- Destroy idle stacks
- Tag resources `Project=fraudguard`

## License

This project is licensed under the [MIT License](LICENSE).
