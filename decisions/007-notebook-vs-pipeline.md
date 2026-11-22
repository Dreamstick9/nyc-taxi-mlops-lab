# 007: What stays in the notebook, and what moves into the pipeline

Stage: S2 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

The notebook (`notebooks/explore.ipynb`) is for looking only: sampling the data, plotting durations by hour and zone, and comparing the static median, v1 and v2 on a 5% sample. It logs to its own MLflow experiment (`trip-duration-notebook`) and never registers a model.

Everything that produces a model someone could serve runs in the Dagster pipeline, as the `train_model` job in `pipelines/src/labpipes/defs/training.py`:
- building the training set (UNLOAD of `staging.trip_features`, window and holdout),
- the zone-pair distance lookup (from the window only),
- fitting v2, logging and registering it with the contract's tags,
- evaluating it against the champion or the v1 baseline on the full holdout,
- the blocking `beats_champion` gate and the `challenger` alias.

The `feature_contract` check also runs in the pipeline, on every month `ingest_month` loads.

## Rejected alternative

Train and register the model from the notebook, and use the pipeline only to load data. It is faster to iterate on, since there is no image build or deploy between experiments.

## Why

<!-- Student: rewrite in your own words. Agent assumption to confirm: the notebook keeps the three-model comparison only as exploration, and the pipeline's eval_report is the comparison that counts. -->
A notebook run depends on who ran it, which cells ran, in what order, and on what sample. Nobody can repeat it exactly, and nothing stops it from registering a model that skipped the checks. The pipeline runs the same code every time, on the full data (9,452,562 training trips and 3,476,146 holdout trips, against about 5% in the notebook), records the commit it ran from (`git_sha` on every version), and can only set `challenger` behind the blocking gate. The cost is a slower loop for trying ideas: a change to the model needs a commit, an image build and a deploy before the pipeline runs it.

## What would show this was wrong

An idea that only ever gets tried in the notebook because the pipeline loop is too slow, or a model in MLflow that nobody can trace back to a pipeline run. Also: the notebook's sample numbers drifting far from the pipeline's (for example v2's notebook MAE far from 3.64 minutes) would mean the sample is misleading the exploration.
