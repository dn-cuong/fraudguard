# FraudGuard

Payment fraud scoring pipeline on AWS.

Ingest hits API Gateway → Kinesis, then Go workers on EC2 score each txn using
shared Redis velocity counters (ElastiCache) and DynamoDB history. Target is
sub-80ms p50 under load and 1k+ TPS in the loadgen harness.

```
client → API Gateway → Kinesis → Go workers (EC2)
                                    ├─ ElastiCache (velocity)
                                    └─ DynamoDB (history)
                                         ↓
                                   CloudWatch heartbeat alarms
```

More detail in [docs/architecture.md](docs/architecture.md).

## Local

Needs Go 1.22+ and Docker.

```bash
docker compose up -d redis dynamodb
make build
./bin/ingest -config config/local.yaml -mode score
```

Smoke:

```bash
curl -s -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-1","user_id":"u1","amount":42,"merchant":"Cafe","country":"US","ip":"1.2.3.4"}'
```

Loadgen (1k TPS):

```bash
./bin/loadgen -addr http://localhost:8080 -tps 1000 -duration 15s -workers 64
```

```bash
make test      # unit tests
make up/down   # redis + dynamodb-local
```

## Layout

```
cmd/ingest     HTTP ingest (inline score or put to Kinesis)
cmd/worker     Kinesis consumer (goroutine pool + channels)
cmd/loadgen    TPS / latency harness
internal/      engine, velocity, history, stream, scorer
config/        local.yaml
terraform/     VPC, stream, redis, dynamo, workers, alarms
ansible/       ship worker binary + systemd
scripts/       smoke / localstack helpers
```

## Rules

| Rule | Signal | Store |
|------|--------|-------|
| AMOUNT_HIGH | amount ≥ threshold | config |
| VELOCITY_CARD | txns / card / window | Redis |
| VELOCITY_IP | txns / IP / window | Redis |
| HISTORY_DISPUTE | prior dispute on card | DynamoDB |

score ≥ 80 → DECLINE, ≥ 40 → REVIEW, else ALLOW.

## AWS

```bash
brew install awscli terraform ansible
aws configure --profile fraudguard

cd terraform
cp terraform.tfvars.example terraform.tfvars   # set key_name
terraform init && terraform apply

# linux binary for EC2
GOOS=linux GOARCH=amd64 go build -o bin/worker ./cmd/worker

cd ../ansible
cp inventory.example.yml inventory.yml         # fill from terraform output
ansible-playbook -i inventory.yml site.yml --private-key ~/.ssh/fraudguard.pem
```

Tear down when idle (`terraform destroy`) — ElastiCache + NAT are the cost drivers.

Optional LocalStack Kinesis:

```bash
docker compose --profile aws up -d
# set kinesis.endpoint in config/local.yaml
./bin/ingest -mode kinesis
./bin/worker
```

## Notes

- don't commit `.env`, real `*.tfvars`, or AWS keys
- tag resources with `Project=fraudguard`
