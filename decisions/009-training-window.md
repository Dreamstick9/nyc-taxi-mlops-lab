# 009: Training window

Stage: S2 · Date: 2026-11-22 · Author: Dreamstick9 · Drafted by agent: yes

## Choice

Train on 2024-01, 2024-02 and 2024-03. Test on 2024-04, the month after the window.

The zone-pair distance lookup (`est_distance_miles`) is built from the window only, never from April. The values are the defaults of `TrainingWindow` in `pipelines/src/labpipes/defs/training.py` (`window_start="2024-01"`, `window_end="2024-03"`, `holdout_month="2024-04"`), and the pipeline refuses a holdout that is not after the window.

Rows used (from `staging.trip_features`): 2,931,052 (Jan) + 2,976,570 (Feb) + 3,544,940 (Mar) = 9,452,562 training trips; 3,476,146 holdout trips (Apr).

## Rejected alternative

Train on 2024-02 and 2024-03 only, and test on 2024-04. That uses the most recent months and drops January, whose winter and post-holiday traffic may not look like April.

## Why

<!-- Student: rewrite in your own words. Point to confirm: you chose Jan–Mar / Apr on 2026-11-21. -->
Testing on the month after training is what the model faces in real use: it always predicts trips from a month it has not seen. A random split of the same months would let it see the same days and events it is tested on, and would overstate how good it is. Three months instead of two gives about 9.5 million trips instead of 6.5 million, so rare zone pairs (outer boroughs, airports) have more examples; those are the slices where version 1 is weakest (outer 7.42 min, airport 5.98 min, against 3.64 min overall). The cost: January may teach patterns April does not have, and there is no month left over for a second check, since April is the newest data.

## What would show this was wrong

A model trained on Feb–Mar scoring a lower April MAE than the Jan–Mar one (both can be run from the Dagster launchpad by overriding `ops.training_set.config`). Or, once May lands, the model's error on May being clearly worse than on April, which would mean older months are hurting and the window should move forward or shrink.
