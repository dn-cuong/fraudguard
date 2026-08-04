#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"

echo "POST /v1/payments ..."
RESP=$(curl -sf -X POST "$BASE/v1/payments" \
  -H 'Content-Type: application/json' \
  -d '{
    "card_id": "card-smoke-1",
    "user_id": "user-1",
    "amount": 42.50,
    "currency": "USD",
    "merchant": "CoffeeShop",
    "country": "US",
    "ip": "203.0.113.10"
  }')
echo "$RESP"

TXN_ID=$(echo "$RESP" | sed -n 's/.*"txn_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
if [[ -z "$TXN_ID" ]]; then
  echo "failed to parse txn_id from response" >&2
  exit 1
fi

echo "GET /v1/payments/$TXN_ID (poll until scored) ..."
for i in $(seq 1 30); do
  STATUS_RESP=$(curl -sf "$BASE/v1/payments/$TXN_ID")
  STATUS=$(echo "$STATUS_RESP" | sed -n 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
  if [[ "$STATUS" == "scored" ]]; then
    echo "$STATUS_RESP"
    echo "ok"
    exit 0
  fi
  # score mode returns decision inline; still pollable via DynamoDB
  if echo "$STATUS_RESP" | grep -q '"decision"'; then
    echo "$STATUS_RESP"
    echo "ok"
    exit 0
  fi
  sleep 0.2
done

echo "timed out waiting for score: $STATUS_RESP" >&2
exit 1
