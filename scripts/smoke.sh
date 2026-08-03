#!/usr/bin/env bash
set -euo pipefail

echo "POST /v1/payments ..."
curl -sf -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{
    "card_id": "card-smoke-1",
    "user_id": "user-1",
    "amount": 42.50,
    "currency": "USD",
    "merchant": "CoffeeShop",
    "country": "US",
    "ip": "203.0.113.10"
  }'
echo
echo "ok"
