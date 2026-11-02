# Deploying to AWS

Terraform provisions API Gateway, Kinesis, ElastiCache, DynamoDB (transactions with `txn_id-index` and `dispute-index`, plus the lease and dead-letter tables), EC2 workers, a bastion and CloudWatch alarms. Ansible installs the worker binary as a systemd service.

> This path has passed `terraform validate` but has **not been applied to real AWS yet**. Expect to fix things on the first run.

**Cost:** the NAT gateway and ElastiCache dominate. For a demo, apply, smoke-test and `terraform destroy` in one sitting.

## 1. Tooling

```bash
brew install awscli terraform ansible
aws configure --profile fraudguard
export AWS_PROFILE=fraudguard
aws sts get-caller-identity
```

## 2. Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set key_name and ssh_ingress_cidr
terraform init && terraform apply
terraform output payments_url
terraform output payment_status_url_template
```

## 3. Workers

```bash
GOOS=linux GOARCH=amd64 go build -o bin/worker ./cmd/worker
cd ansible
cp inventory.example.yml inventory.yml         # fill in from terraform output
ansible-playbook -i inventory.yml site.yml --private-key ~/.ssh/fraudguard.pem
```

## 4. Client flow

```bash
BASE="$(cd terraform && terraform output -raw payments_url)"

curl -s -X POST "$BASE" \
  -H 'Content-Type: application/json' \
  -d '{"card_id":"card-1","user_id":"u1","amount":42,"merchant":"Cafe","country":"US","ip":"1.2.3.4"}'
# {"status":"queued","txn_id":"...","poll":"/payments/..."}

curl -s "$BASE/TXN_ID"
# {"status":"scored","decision":"ALLOW","score":0,...}
```

On AWS, `POST /payments` rejects unknown fields (including `txn_id` and `timestamp`) with 400, and returns 502 if the payment couldn't be put on the stream.

## 5. Teardown

```bash
cd terraform && terraform destroy
```

## Safety

- Never commit `.env`, real `*.tfvars`, access-key CSVs or PEM keys.
- Set `ssh_ingress_cidr` in `terraform.tfvars` to your IP/32 before applying. The default is open, for demos.
- Destroy idle stacks.
- Resources are tagged `Project=fraudguard`.
