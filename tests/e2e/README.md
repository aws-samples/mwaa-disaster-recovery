# MWAA DR — End-to-End Test Framework

Python orchestrator that provisions real MWAA environments across multiple
Airflow versions, deploys the DR solution, simulates a disaster, verifies
recovery, and tears everything down. It never modifies repository code —
it only exercises the solution as-is.

## Usage

```bash
cd tests/e2e

./run_e2e.py --dry-run            # print the execution plan, touch nothing
./run_e2e.py                      # full end-to-end run (all configured versions)
./run_e2e.py --versions 2.11.0    # test a single version
./run_e2e.py --sequential         # force sequential even if config says parallel
./run_e2e.py --teardown           # remove a version's resources after it PASSES
./run_e2e.py --provision-infrastructure  # force re-provisioning of all infra
./run_e2e.py --cleanup-only       # delete ALL framework resources and exit
```

Iterate-by-default: the framework exists to help develop the DR solution, so
infrastructure is KEPT after every run — pass or fail. The first run
provisions MWAA environments (~1 h); every subsequent `./run_e2e.py` adopts
the existing AVAILABLE environments, redeploys the DR solution, and reruns
all checks in minutes. Use `--teardown` for CI-style runs that should remove
a version's resources on PASS (shared VPCs go when everything passed), and
`--cleanup-only` to remove everything at any time. Pass
`--provision-infrastructure` to force the full provisioning path.

Failure behavior: a FAILed version's resources are always kept (even with
`--teardown`) so you can fix the problem and rerun — provisioning is
idempotent: existing buckets/roles/envs are reused, envs stuck in
CREATE_FAILED are deleted and recreated, and AVAILABLE envs are adopted
directly.

Ctrl+C terminates all child processes (CDK subprocesses included) — no
zombies. Resources already created stay; remove them with `--cleanup-only`.

## Configuration (`e2e_config.yaml`)

| Key | Purpose |
|---|---|
| `test_run.id_prefix` | Prefix for every resource name (also the cleanup tag) |
| `test_run.parallel` | Run version tests in parallel threads |
| `aws.primary_region` / `aws.secondary_region` | Regions under test |
| `versions` | MWAA versions to test (must be in `config.py` SUPPORTED_MWAA_VERSIONS) |
| `dr_strategies` | `WARM_STANDBY` and/or `BACKUP_RESTORE` |
| `mwaa.*` | Environment class / worker counts |
| `bedrock.*` | AI result summary (model id configurable) |
| `timeouts.*` | Phase timeouts |

The AWS account is always auto-detected from active credentials
(`sts get-caller-identity`) — it is not configurable.

## What a version test does

1. Shared infra (once per run): 1 VPC per region (IGW, NAT, 2 private subnets,
   self-referencing SG) — shared by all versions to stay within EC2 limits.
2. Per version: DAGs buckets (versioned, seeded with `requirements.txt` and
   an example workload DAG under `dags/`) + MWAA execution roles (idempotent —
   reused if they exist). The DR framework DAGs (`backup_metadata` etc.) are
   deployed into the bucket later, by the CDK primary stack (step 4).
3. Create primary + secondary MWAA environments, wait for AVAILABLE (~30 min).
4. `cdk deploy --all` of the DR solution with a per-version stack prefix and
   isolated `cdk.out.e2e-<version>` output dir.
5. Seed a marker Airflow variable in the primary environment.
6. Trigger `backup_metadata`, wait for backup CSVs in the primary backup
   bucket, then for cross-region replication.
7. Start the recovery Step Functions with `{"simulate_dr": "YES"}` (the
   documented manual trigger) and wait for `SUCCEEDED`.
8. Verify the marker variable exists in the secondary environment.
9. Cleanup: destroy CDK stacks, delete MWAA environments, buckets, roles.
   Shared VPCs are deleted at the very end.

## Output

- Live progress board printed every 20 s while tests run.
- `logs/<timestamp>/e2e.log` — full log; `cdk_*.log` — CDK output per version.
- `logs/<timestamp>/test_report.json` — machine-readable results
  (per-version result, per-check status, timings, errors).
- Terminal results table.
- Optional Bedrock AI summary (`bedrock_summary.txt`).

Checks recorded per version: `seed_marker`, `backup_created`, `replication`,
`dr_workflow`, `marker_restored`.

## Expected duration & cost

Single version ≈ 1.5–2 h (MWAA create ~30 min, delete ~25 min dominate).
Four versions in parallel ≈ 2–2.5 h wall clock. Cost is dominated by MWAA
environment hours (mw1.small ≈ $0.49/h each) plus 2 NAT gateways.

## Requirements

- Python 3.9+, `boto3`, `pyyaml`
- Node.js + CDK (`npx cdk`) — for deploying the DR stacks
- Long-lived AWS credentials (Admin) — a full run exceeds 1 h; expiring
  session tokens will break the run mid-flight.
