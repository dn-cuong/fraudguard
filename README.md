# FraudGuard

[![CI](https://github.com/dn-cuong/fraudguard/actions/workflows/ci.yml/badge.svg)](https://github.com/dn-cuong/fraudguard/actions/workflows/ci.yml)
[![Security](https://github.com/dn-cuong/fraudguard/actions/workflows/security.yml/badge.svg)](https://github.com/dn-cuong/fraudguard/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Real-time card fraud scoring on AWS: every payment gets `ALLOW`, `REVIEW` or `DECLINE` from a pool of stateless Go workers that share one view of each card.**

Payments enter through API Gateway into Kinesis. Go workers score them against velocity counters in Redis and card history in DynamoDB, and the client polls for the decision. Infrastructure is Terraform, workers are deployed with Ansible, and CloudWatch alarms on missing heartbeats and unscorable records.

> **Status:** a side project about what happens when several scorer replicas share state. It's rules only, no ML. It has been deployed and tested on real AWS with Terraform, then torn down to save cost, and everything also runs locally against Docker (Redis, DynamoDB Local, LocalStack Kinesis).

## Quick start

Needs Docker and Go 1.26.10+. This runs the same path as AWS, with ingest → Kinesis → worker → DynamoDB, all in containers:

```bash
git clone https://github.com/dn-cuong/fraudguard && cd fraudguard
make stack-smoke    # build images, start the stack, send a payment, poll until it's scored
```

Then send your own:

```bash
curl -s -X POST localhost:8080/v1/payments -H 'Content-Type: application/json' \
  -d '{"card_id":"card-hot","amount":3000,"merchant":"Luxury","country":"US","ip":"9.9.9.9"}'
# {"status":"queued","txn_id":"9f1c…","poll":"/v1/payments/9f1c…",…}

curl -s localhost:8080/v1/payments/9f1c…
# {"status":"scored","decision":"REVIEW","score":40,…}      AMOUNT_HIGH: 3000 ≥ 2500
```

`make down` stops everything. For a faster loop without Kinesis, `make up && make ingest` scores inline and returns the decision straight from the `POST`.

## Features

- **Consistent across replicas.** Workers keep nothing in memory. Counters live in Redis and history in DynamoDB, so any worker scores a payment the same way.
- **Safe under replays and crashes.** Counting is atomic and per-txn idempotent, results are written once, and a batch is checkpointed only when every record in it has a result.
- **Elastic workers.** Kinesis shards are shared through DynamoDB leases. Add or remove workers freely; a dead worker's shards are picked up from its last checkpoint after about 30s.
- **Nothing silently lost.** Failing scores are retried, then stored as `failed` for the client, and the raw record goes to a dead-letter table with an alarm.
- **Velocity on arrival time.** A backlog drained after an outage isn't mistaken for a fraud burst, and client-supplied timestamps are ignored.
- **Configurable rules.** Thresholds and weights are YAML; the engine is pure, with no I/O, and unit-tested.

## Rules

| Rule | Fires when | Default weight |
|------|------------|---------------:|
| `AMOUNT_HIGH` | amount ≥ $2,500 | 40 |
| `VELOCITY_CARD` | more than 5 txns on the card in the last minute | 50 |
| `VELOCITY_IP` | more than 20 txns from the IP in the last minute | 30 |
| `HISTORY_DISPUTE` | the card has a disputed txn | 60 |

Weights add up: **≥ 80 `DECLINE`**, **≥ 40 `REVIEW`**, otherwise `ALLOW`. Every threshold and weight is in [`config/local.yaml`](config/local.yaml).

## API

| | Local ingest | AWS (API Gateway) |
|---|---|---|
| Submit a payment | `POST /v1/payments` | `POST /payments` |
| Poll for the decision | `GET /v1/payments/{txn_id}` | `GET /payments/{txn_id}` |
| Mark a txn disputed | `POST /v1/payments/{txn_id}/dispute` | not yet |

A payment needs `card_id` and `amount`. `currency` (USD only), `user_id`, `merchant`, `country` and `ip` are optional. `txn_id` and `timestamp` are set by the server. Marking a dispute takes `{"card_id": "…"}`, the card the txn was on. Polling returns `pending` until a worker has scored the payment, then `scored` with `decision` and `score`, or `failed` with the reason.

## Performance

One local run on an Apple M3 Pro, scoring inline (`-mode score`, no Kinesis): 1,000 TPS for 15s, **p50 1.3 ms, p99 31 ms**, no errors. It says nothing about latency on AWS. Reproduce with `make ingest` and `make loadgen`.

Failover was checked locally with two workers on LocalStack: killed workers, reshards under load, Redis outages and payments sent before any worker was up were all scored. The details are in [docs/architecture.md](docs/architecture.md#failover-checked-locally).

## Documentation

- [Architecture](docs/architecture.md): data path, why stateless, storage design, leases and checkpoints, how velocity is counted, known limitations.
- [Deploying to AWS](docs/aws.md): Terraform, Ansible, the client flow, cost and safety notes.
- [Contributing](CONTRIBUTING.md): local checks, tests that need DynamoDB Local, what CI requires.

## Roadmap

Planned work is tracked in [GitHub issues](https://github.com/dn-cuong/fraudguard/issues): authentication and throttling on the API, more rules with versioned rulesets and a shadow mode, a backtesting harness, latency histograms and tracing, card-ID tokenization, and moving workers to ECS Fargate.

## License

[MIT](LICENSE)
