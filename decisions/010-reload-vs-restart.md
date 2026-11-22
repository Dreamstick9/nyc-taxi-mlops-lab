# 010: Load at start-up, or reload on alias change

Stage: S3 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

Serving picks up a new champion by reloading, not by restarting. After `promote.yaml` moves the `champion` alias in MLflow, it calls `deploy/web.sh reload champion`, which sends `POST /reload` to the `serving-champion` container. The container loads the version the alias now points at, warms it up with one prediction, and only then swaps it in. Until the swap, the old model keeps answering.

Start-up loading still exists: a container that starts (after a deploy or a host restart) loads whatever the alias points at then. Reload is how a running container changes model.

First promotion (2026-11-22): version 1 became champion; serving-champion went from "no champion yet" to v1, and the app's `/predict` went from the static median (12.25 min) to `{"version": "1", "alias": "champion"}` (63.12 min for JFK to Times Square, Friday 5pm).

## Rejected alternative

Restart the `serving-champion` container after moving the alias, so it loads the new version at start-up. Simpler: no live swap, and every model change goes through the same path as a deploy.

## Why

<!-- Student: rewrite in your own words. Point to confirm: you chose reload on 2026-11-22. -->
A restart leaves a gap. While the container stops, starts, downloads the model (5.3 MB) and loads it, it has no model, Traefik's health check on `/model` takes it out of rotation, and the app falls back to the static median. Riders would get 12.25 minutes for every trip for that time. With reload the old model keeps answering until the new one is loaded and warmed up, so riders see no gap and no fallback. Rolling the model back is the same action: move the alias back and reload. The cost is one more moving part: the promotion has to call `/reload`, and if it doesn't (or the call fails), serving keeps the old model while MLflow says the new one is champion.

## What would show this was wrong

`deploy/web.sh status` showing serving-champion on a different version than MLflow's `champion` alias, for example after a reload that failed and nobody noticed. Or a reload that makes `/predict` slow or failing while the new model loads, which would mean the swap is not as seamless as assumed. In S8, timing a model rollback by reload will show whether it is really faster than a restart.
