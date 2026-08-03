#!/usr/bin/env bash
set -euo pipefail
awslocal kinesis create-stream --stream-name fraudguard-payments --shard-count 2 || true
echo "LocalStack Kinesis stream ready: fraudguard-payments"
