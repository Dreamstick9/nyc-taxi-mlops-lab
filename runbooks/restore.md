# Runbook: bring the lab back after a session end

Owner: Dreamstick9 · Last run: 2026-11-22 · Time to restore last run: about 7 min (host boot 10:21:21 UTC to `scripts/health.sh` exit 0 at 10:28 UTC)

## When to use it

After Start Lab, or when someone says "the app quotes 12.25 minutes for every trip", "Dagster won't load", or "health.sh says FAIL". The Learner Lab session ends at the 4-hour mark: it stops the EC2 hosts and expires the laptop's AWS keys. RDS, Redshift and the notebook stay however they were left (at 18:45 we stop RDS and the notebook and pause Redshift).

## Before you start

- **AWS keys:** Learner Lab → AWS Details → AWS CLI. Paste them into `~/.aws/credentials`. They expire when the session ends.
- **GitHub:** `gh` logged in as the repo owner (`gh auth status`).
- **Repo:** `~/nyc-taxi-mlops-lab`, with `infra/live` initialised (`health.sh` reads Terraform outputs).

## Steps

Times are from 2026-11-22: the morning restore (after the overnight stop, about 04:54 UTC) and the midday one (session end at the 4-hour mark, hosts back at 10:21 UTC).

| # | Check (command) | Healthy looks like | If not: fix (command) | Took | Automated? |
| --- | --- | --- | --- | --- | --- |
| 1 | `aws sts get-caller-identity` | prints the account | paste the new keys from AWS Details into `~/.aws/credentials` | about 1 min | no: a person has to copy them from Learner Lab |
| 2 | `scripts/health.sh` (lines `app`, `platform`, `llm`) | `running`, `runner online` | `aws ec2 start-instances --instance-ids <ids from terraform output>` | 0: Start Lab started them; containers and runners were up 8 s after boot (10:21:21 → 10:21:29) | yes: Start Lab + Docker `restart: unless-stopped` + the runner's systemd service |
| 3 | `scripts/health.sh` (line `rds`) | `available` | `aws rds start-db-instance --db-instance-identifier lab` | 3 min (requested about 04:54:30, available 04:57:30) | yes: `wake` on the platform host (decision 012) |
| 4 | `scripts/health.sh` (line `redshift`) | `available` | `aws redshift resume-cluster --cluster-identifier lab` | 2 min 49 s (04:54:26 → 04:57:15) | yes: `wake` on the platform host (decision 012) |
| 5 | Dagster: `curl -s 127.0.0.1:3000/server_info` through the SSM tunnel | HTTP 200 | nothing: webserver and daemon restart until RDS answers | about 1 min after RDS was available | yes: `restart: unless-stopped` |
| 6 | MLflow: `curl -s 127.0.0.1:5000/health` through the SSM tunnel (use 127.0.0.1, not localhost, on a Mac) | HTTP 200 | nothing: it restarts until RDS answers | came back with Dagster | yes: `restart: unless-stopped` |
| 7 | `scripts/health.sh` (line `predict`) | `trip-duration champion <version>` | `deploy/web.sh reload champion` on the app host | 0 at midday: serving-champion loaded the alias at start-up | yes: serving loads its alias at start-up and retries until MLflow answers |
| 8 | `scripts/health.sh` (line `status`) | `Dagster answered the status run N min ago` | `gh workflow run status.yaml --repo Dreamstick9/nyc-taxi-mlops-lab` | 14 s (10:27:55); FAIL before it, because the overnight scheduled runs were cancelled while the hosts were off | partly: the 20-minute schedule fixes it by itself; run it by hand to see green now |
| 9 | `scripts/health.sh` (line `notebook`) | `Stopped` is fine | `aws sagemaker start-notebook-instance --notebook-instance-name lab` (about 6 min) | not done: not needed for serving | no: on purpose (decision 012) |

On 2026-11-22 the morning restore was done by hand before this runbook existed and `health.sh` was not run; its times come from the RDS and Redshift event logs (`aws rds describe-events`, `aws redshift describe-events`) and container start times. The midday restore was written down as it happened.

## Done when

1. A rider gets a model answer: `curl -X POST http://100.51.213.71/predict -H 'Content-Type: application/json' -d '{"pickup_zone_id": 132, "dropoff_zone_id": 230, "pickup_datetime": "2024-03-08T17:05:00"}'` returns `"alias": "champion"`.
2. `scripts/health.sh` exits 0.
3. The status workflow is green after the restore, and reports RDS and Redshift `available`.

## What we automated, and what we left as a step

See [decision 012](../decisions/012-what-to-automate.md).

Automated: starting RDS and resuming Redshift (`wake` on the platform host, at every boot), plus what already came back by itself (hosts, containers, runners, Dagster, MLflow, serving, the status schedule).

Left to a person:
- **AWS keys on the laptop:** they come from the Learner Lab page; nothing in AWS can fetch them.
- **The notebook:** it costs money and is only for exploration; start it when you need it.
- **Anything that changes infrastructure, a model alias or the app's live colour:** Terraform, `promote.yaml` (approval-gated) and the CD rollback button stay with a person.
- **Checking:** run `scripts/health.sh` once after Start Lab, even when everything should have come back.
