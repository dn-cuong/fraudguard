## What & why
<!-- What does this change, and why? Link the issue: Closes #123 -->

## Type
- [ ] Bug fix
- [ ] Feature
- [ ] Refactor / cleanup
- [ ] Infra (Terraform / Ansible)
- [ ] Docs / CI

## How I tested
<!-- e.g. `make test`, `make smoke`, loadgen numbers, terraform plan -->

## Checklist
- [ ] `make test` passes (with `-race`)
- [ ] Scoring changes keep `engine` pure and workers stateless
- [ ] Infra changes: `terraform plan` reviewed, no secrets / `*.tfvars` committed
- [ ] README / config docs updated if behavior or endpoints changed
