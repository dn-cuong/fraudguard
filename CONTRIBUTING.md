# Contributing

Branch from `main` and open a PR; direct pushes are for small docs fixes only. The PR template lists what reviewers look for.

## Before you push

```bash
gofmt -l .                       # must print nothing
go vet ./...
make up                          # redis + dynamodb-local
DYNAMO_TEST_ENDPOINT=http://localhost:8000 go test -race ./...
```

Without `DYNAMO_TEST_ENDPOINT` the lease and history tests skip rather than fail, so a green `make test` doesn't cover them.

## Checks on every PR

| Workflow | Job | What it does |
|----------|-----|--------------|
| CI | Go | gofmt, `go mod tidy` is clean, vet, `go test -race` with DynamoDB Local, build, Linux cross-compile |
| CI | golangci-lint | config in `.golangci.yml` |
| CI | Terraform | `fmt -check`, `validate` |
| CI | Ansible lint | `ansible/site.yml` |
| Security | govulncheck, tfsec, gitleaks | tfsec is soft-fail, the other two block |

All CI jobs have to pass before merging.

## Rules that are easy to break

- `internal/engine` stays pure: no I/O, so any worker with the same snapshot gives the same decision.
- Workers stay stateless, and scoring stays idempotent because batches replay.
- Never commit `.env`, real `*.tfvars`, access-key CSVs or PEM keys.
