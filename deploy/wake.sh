#!/bin/sh
# Boot-time wake (S4, decision 012). Runs in the `wake` container on the platform host, which Docker
# starts at every boot (restart: unless-stopped), as the host's instance profile (LabRole).
#
# It does exactly two things, and only when they're needed:
#   - RDS `lab` stopped (or stopping)   -> start it
#   - Redshift `lab` paused (or pausing) -> resume it
# then waits until both are available and logs how long that took. After that it sleeps until the next
# boot. Dagster, MLflow and serving need no help: they come back by themselves once RDS answers.
#
# What it must never do (decision 012): start the SageMaker notebook, create or change infrastructure,
# move a model alias, or switch the app's colours. LabRole would allow some of that, so the limit is
# this file: these four API calls (describe and start/resume, for two named resources) and nothing else.
#
# Logs: docker logs platform-wake-1
set -u

RDS_ID=${RDS_ID:-lab}
REDSHIFT_ID=${REDSHIFT_ID:-lab}
WAIT_SECONDS=${WAIT_SECONDS:-1200}

log() { echo "$(date -u +%FT%TZ) wake: $*"; }
rds_state() { aws rds describe-db-instances --db-instance-identifier "$RDS_ID" --query 'DBInstances[0].DBInstanceStatus' --output text 2>/dev/null || echo unknown; }
redshift_state() { aws redshift describe-clusters --cluster-identifier "$REDSHIFT_ID" --query 'Clusters[0].ClusterStatus' --output text 2>/dev/null || echo unknown; }

start=$(date +%s)
deadline=$((start + WAIT_SECONDS))
rds_asked=no redshift_asked=no
log "boot: RDS $(rds_state), Redshift $(redshift_state)"

while :; do
  rds=$(rds_state)
  redshift=$(redshift_state)

  case "$rds" in
    stopped)
      if [ "$rds_asked" = no ]; then
        if aws rds start-db-instance --db-instance-identifier "$RDS_ID" >/dev/null; then log "RDS: start requested"; rds_asked=yes
        else log "RDS: start-db-instance failed, will retry"; fi
      fi ;;
    stopping) ;;  # can't start until it has stopped: wait
  esac

  case "$redshift" in
    paused)
      if [ "$redshift_asked" = no ]; then
        if aws redshift resume-cluster --cluster-identifier "$REDSHIFT_ID" >/dev/null; then log "Redshift: resume requested"; redshift_asked=yes
        else log "Redshift: resume-cluster failed, will retry"; fi
      fi ;;
    pausing) ;;  # can't resume until it has paused: wait
  esac

  if [ "$rds" = available ] && [ "$redshift" = available ]; then
    log "RDS and Redshift available after $(( $(date +%s) - start ))s (start requested: RDS $rds_asked, Redshift $redshift_asked)"
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    log "gave up after ${WAIT_SECONDS}s: RDS $rds, Redshift $redshift. Follow runbooks/restore.md"
    break
  fi
  sleep 15
done

# Nothing more to do until the next boot. Sleeping (not exiting) keeps restart: unless-stopped from
# running this again in a loop.
exec sleep infinity
