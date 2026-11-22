# 008: Promotion metric and margin

Stage: S2 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

A new version becomes `challenger` only if both hold, on the same holdout month:
- **Accuracy:** its MAE (`mae_min`, average absolute error in minutes) is lower than the comparison's by any amount. Margin 0; an equal MAE fails. The comparison is the current `champion`, or the v1 baseline while there is no champion.
- **Speed:** its p95 single-row prediction latency (`p95_latency_ms`) is at most 20 ms.

The values are the defaults of `PromotionRule` in `pipelines/src/labpipes/defs/training.py` (`mae_margin_min=0.0`, `p95_latency_budget_ms=20.0`). The gate is the blocking `beats_champion` check on `eval_report`.

First run (2026-11-21, version 1): MAE 3.64 min against the v1 baseline's 3.96 min (0.32 min, 8.1% better); p95 4.75 ms. It passed and became `challenger`.

## Rejected alternative

A relative margin: the new version must be at least 2% (or 5%) better than the comparison. That filters out gains too small to be more than noise.

## Why

<!-- Student: rewrite in your own words. Points to confirm: margin 0 (any strict improvement) and the 20 ms budget are your choices from 2026-11-21. -->
MAE, not RMSE, because a few trips take hours and RMSE would let those few dominate; MAE reads directly as "minutes off on a typical trip". Any improvement is enough because `challenger` is not what serves: the approval-gated promotion (S3) still has to move `champion`, so a person looks before a tiny gain reaches riders. The holdout is large (3,476,146 trips), so a measured difference on it is unlikely to be pure chance. The latency budget keeps the model a small part of a quote request: 20 ms is tight, and the first version used under a quarter of it.

The cost of margin 0: a version that is better by a few seconds still becomes `challenger`, so challengers may change often for gains that do not matter to a rider.

## What would show this was wrong

`challenger` changing on most runs for improvements of a few seconds, or a challenger that wins overall while getting worse on a slice that matters (airport MAE was 5.98 min and outer-borough MAE 7.42 min on version 1). On latency: the pipelines Dockerfile records p95 at 19.6 ms without its `OMP_WAIT_POLICY=PASSIVE` setting, just under the budget. A p95 close to 20 ms would mean a heavier model, or that setting going missing, starts failing the gate.
