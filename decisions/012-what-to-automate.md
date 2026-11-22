# 012: What to automate after a session end, and what to leave as a step

Stage: S4 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

A `wake` container on the platform host (`deploy/compose.platform.yaml`, script `deploy/wake.sh`). Docker starts it at every boot, like the other containers. Running as LabRole, it:
- starts RDS `lab` if it is stopped,
- resumes Redshift `lab` if it is paused,
- waits until both are available, logs how long that took, then sleeps until the next boot.

**Trigger:** the platform host booting, which happens when Start Lab starts the hosts. No laptop, no keys and no person needed.

**It must never, on its own:**
- start the SageMaker notebook,
- create or change infrastructure (no Terraform, no new resources),
- move a model alias (`champion` or `challenger`),
- switch the app's blue-green colour.

LabRole could do some of those, so the limit is in the code: `wake.sh` only calls describe, start and resume, on two named resources.

Left as steps in `runbooks/restore.md`: pasting the laptop's AWS keys, starting the notebook when needed, and running `scripts/health.sh` once to check.

## Rejected alternative

A laptop script (`scripts/restore.sh`) that I run after Start Lab and pasting keys: start RDS, resume Redshift, wait, run the status workflow and `health.sh`. It keeps a person deciding when Redshift starts costing money.

## Why

<!-- Student: rewrite in your own words. Points to confirm: boot-time wake on the platform host, and the four "never" limits, were your choices on 2026-11-22. -->
On 2026-11-22, RDS (3 min) and Redshift (2 min 49 s) were the only things that needed a person after Start Lab. Everything else came back by itself: the hosts, every container and both runners were up 8 s after boot, Dagster and MLflow restarted once RDS answered, serving loaded the champion at start-up, and the status schedule fixes itself within 20 minutes. So the gap was two API calls that are the same every morning: classic toil. Doing them at boot means the system heals from Start Lab alone, even before I've copied the new keys; a laptop script would still need me there first.

The limits come from what can't be undone cheaply. The notebook costs money and is only for exploration; infrastructure, model aliases and the live colour each already have their own owner (Terraform with review, the approval-gated promotion, CD and the rollback button). A start-up script that changes them would be a second, unreviewed way to do things that are meant to be reviewed.

The cost: Redshift (about $0.54 an hour) resumes at every boot, including on a day I didn't plan to use it, and a platform redeploy restarts `wake`, which would resume a cluster I had paused on purpose.

## What would show this was wrong

A morning where Redshift was resumed without being needed and the cost shows up, or a cluster I paused on purpose coming back after a platform deploy. Or the opposite: `docker logs platform-wake-1` showing a start or resume that failed (for example LabRole being denied `rds:StartDBInstance`), so the lab still needed the runbook by hand. S4's "prove it" step (pause Redshift, reboot the platform host, touch nothing) is the test.
