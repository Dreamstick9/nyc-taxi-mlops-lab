# 013: SLO targets

Stage: S5 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

Three SLOs on the rider-facing `/predict` route, each over 30 days (`observability/slo.yaml`, computed by `observability/rules/slo.rules.yaml`):

| SLO | SLI (from `contracts/metrics.yaml`) | Objective | Alert |
| --- | --- | --- | --- |
| predict-availability | `/predict` requests that are not 5xx (`lab_http_requests_total`) | 99% | `PredictAvailabilityBurnRate` |
| predict-latency | `/predict` requests answered in under 250 ms (`lab_http_request_duration_seconds`, bucket `le="0.25"`) | 99% | `PredictLatencyBurnRate` |
| predict-model-answered | quotes from the model, not the 12.25 fallback (`lab_predictions_total{source="model"}`) | 99% | `PredictModelAnsweredBurnRate` |

Each alert is multi-window, multi-burn-rate on a 1% error budget: it **pages** when the budget burns 14.4x too fast over both 1 h and 5 min (`for: 2m`), and opens a **ticket** at 6x over both 6 h and 30 min (`for: 15m`). Blue and green, and both hosts, are summed together. A window with no `/predict` traffic records nothing, so no traffic never pages.

## Rejected alternative

Stricter objectives: 99.9% availability (about 43 minutes of failure a month) and 99% under 100 ms.

## Why

<!-- Student: rewrite in your own words. Points to confirm: 99% / 99% under 250 ms / 99% model-answered were your choices on 2026-11-22; the alert windows (14.4x over 1h+5m, 6x over 6h+30m) and the for: durations (2m, 15m) were chosen by the agent. -->
This lab stops every night and is restarted every morning, deploys several times a day, and runs on one host per role. 99% (about 7 hours of budget a month) leaves room for that without paging on every restart; 99.9% would be used up by one slow rollback. 250 ms is still instant for a rider waiting for a quote: the model itself takes about 5 ms (p95 4.75 ms), so the rest is the app, Traefik and the network. The third SLO exists because availability can't see the worst quiet failure: serving down while the app answers 200 with the 12.25 fallback for everyone. Paging on two windows at once means a 2-minute spike doesn't wake anyone (tested), but a real outage pages within about 10 minutes (tested: errors from minute 61, page at minute 71).

## What would show this was wrong

Pages for things riders didn't notice (objective too strict), or riders noticing something no alert caught. A known blind spot: if the app is down and not scraped at all, there are no `/predict` series, so none of these SLOs fire; an `up`/`absent` alert would cover it. Also: the step 5 test (stopping `serving-champion` under traffic) should fire `PredictModelAnsweredBurnRate`; if it takes much longer than about 10 minutes plus Grafana Cloud's delay, the windows are too slow.
