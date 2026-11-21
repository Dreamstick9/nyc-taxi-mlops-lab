"""S2: training_set, trained_model, eval_report, the challenger gate and the challenger asset.

- training_set: UNLOAD a window of staging.trip_features (and the holdout month) to the artifact bucket.
  The zone-pair distance lookup (contracts/features.yaml, est_distance_miles) comes from the window only.
- trained_model: fit tripmodel.LightGBMModel (the given v2), log it to MLflow as a pyfunc with
  tripmodel.model_signature() and the tripmodel package bundled, register a version of the model named
  in contracts/mlflow.yaml, and tag it as that contract says.
- eval_report: tripmodel.evaluate the new version and the champion (or the v1 baseline before there is
  one) on the same holdout; log the metrics.
- A blocking check that guards a `challenger` asset, which sets the alias. The pipeline never moves
  `champion`.
Decision points: what stays in the notebook; the promotion metric and margin; the training window.

  trip_features --> training_set --> trained_model --> eval_report --(beats_champion, blocking)--> challenger

How the steps hand over: each asset reads the metadata of the latest materialization of the ONE asset
before it (Dagster's event log, as drift.py does), and each carries forward what the next needs. So a
model is always evaluated on the holdout of the training set it was fitted on, and `challenger` only
ever moves to the version the gate just passed. Run the lot as the `train_model` job.

mlflow, lightgbm and tripmodel are imported inside the functions that use them: every step of every
job imports this module, and the ingest steps shouldn't pay for LightGBM.
"""

import os
import re
from collections.abc import Callable

import dagster as dg
import pandas as pd
from pydantic import Field

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData

# ======================================================================================================
# THE STUDENT'S DECISIONS (recorded in decisions/: the training window; the promotion metric and margin).
# These defaults are the only place the values live. They show up in the Dagster launchpad under
# ops.training_set.config and ops.eval_report_beats_champion.config, where a run can override them; a
# run started with no config (scripts/dagster-run.sh train_model) uses exactly these.
# Changing a default changes a graded decision: update its record in decisions/ in the same commit.
# ======================================================================================================


class TrainingWindow(dg.Config):
    """Which months train and which month tests (decision: training window)."""

    window_start: str = Field(default="2024-01", description="First month of the training window, YYYY-MM.")
    window_end: str = Field(default="2024-03", description="Last month of the training window, YYYY-MM (inclusive).")
    holdout_month: str = Field(default="2024-04", description="The month every model is evaluated on, YYYY-MM. Never trained on.")


class PromotionRule(dg.Config):
    """What a new version must do to become `challenger` (decision: promotion metric and margin).

    It passes when its holdout MAE is lower than the comparison's (the champion's on the same holdout,
    or the v1 baseline's before there is a champion) by MORE than mae_margin_min, and its p95
    single-row latency is at most p95_latency_budget_ms. A margin of 0 means strictly lower, by any amount."""

    mae_margin_min: float = Field(default=0.0, description="Minutes of holdout MAE the new version must beat the comparison by (0 = any strict improvement).")
    p95_latency_budget_ms: float = Field(default=20.0, description="Largest allowed p95 latency of a single-row predict, in milliseconds.")


# ======================================================================================================

FEATURES_TABLE = "staging.trip_features"
TRIPS_TABLE = "staging.stg_trips"  # where trip_distance lives: it is forbidden in trip_features
ZONES_TABLE = "ingest.taxi_zones"
CHALLENGER = "challenger"  # the only alias this module ever sets
GATE = "beats_champion"
S3_URI = re.compile(r"s3://[a-z0-9][a-z0-9.-]+/[A-Za-z0-9_./-]+")


# ---- training_set: plain functions -------------------------------------------------------------------


def window_months(first: str, last: str) -> list[str]:
    """'2024-01', '2024-03' -> ['2024-01', '2024-02', '2024-03']. Both ends are validated."""
    first, last = check_month(first), check_month(last)
    if first > last:
        raise ValueError(f"the training window starts after it ends: {first}..{last}")
    y, m = map(int, first.split("-"))
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def plan_split(window_start: str, window_end: str, holdout_month: str) -> tuple[list[str], str]:
    """(window months, holdout month). The holdout has to come after the window: a model tested on a
    month it trained on, or on a month before its training data, isn't tested on the future."""
    months = window_months(window_start, window_end)
    holdout = check_month(holdout_month)
    if holdout <= months[-1]:
        raise ValueError(f"holdout month {holdout} must come after the training window {months[0]}..{months[-1]}")
    return months, holdout


def _months_sql(months: list[str], quote: str = "'") -> str:
    """'2024-01', '2024-02' for an IN list. Every month is validated: batch statements take no parameters."""
    return ", ".join(f"{quote}{check_month(m)}{quote}" for m in months)


def _unload(select: str, uri: str) -> str:
    """UNLOAD a SELECT (its quotes already doubled) to a prefix as Parquet, with the cluster's default
    role. No ALLOWOVERWRITE and no CLEANPATH: a prefix belongs to one run, and a second UNLOAD into it
    fails instead of mixing two training sets."""
    if not S3_URI.fullmatch(uri):
        raise ValueError(f"not an s3:// prefix this pipeline writes to: {uri!r}")
    return f"unload ('{select}') to '{uri}' iam_role default format as parquet"


def feature_columns() -> list[str]:
    """The contract's inputs, then its label: what a training or holdout row is."""
    contract = features_contract()
    return [c["name"] for c in contract["inputs"]] + [contract["label"]["name"]]


def unload_features_sql(months: list[str], uri: str) -> str:
    """Rows of staging.trip_features for these months. The inputs and the label, nothing else."""
    months_in = _months_sql(months, quote="''")  # doubled: the SELECT sits inside UNLOAD's quotes
    return _unload(f"select {', '.join(feature_columns())} from {FEATURES_TABLE} where trip_month in ({months_in})", uri)


def unload_distances_sql(window: list[str], uri: str) -> str:
    """est_distance_miles: the median trip_distance per zone pair over the WINDOW (never the holdout:
    a lookup that saw April would leak April's trips into a model tested on April). The distance is in
    staging.stg_trips only, so the trips are filtered here as trip_features.sql filters them, plus a
    positive distance (as notebooks/explore.ipynb does)."""
    months_in = _months_sql(window, quote="''")
    select = (
        f"select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles from {TRIPS_TABLE} "
        f"where trip_month in ({months_in}) "
        "and duration_min > 0 and duration_min <= 180 "
        "and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263 "
        "and trip_distance > 0 group by 1, 2"
    )
    return _unload(select, uri)


def unload_training_set(redshift: RedshiftData, artifacts: ArtifactBucket, window: list[str], holdout: str, prefix: str) -> dict:
    """UNLOAD train/, holdout/ and distances/ under `prefix` in the artifact bucket, in one transaction
    (one snapshot of the tables). Returns what the materialization records."""
    have = {r["trip_month"] for r in redshift.query(f"select distinct trip_month from {FEATURES_TABLE} order by 1")}
    missing = [m for m in [*window, holdout] if m not in have]
    if missing:
        raise dg.Failure(description=f"{FEATURES_TABLE} has no rows for {', '.join(missing)}: materialize trip_features for those months first")
    redshift.execute(
        unload_features_sql(window, artifacts.uri(f"{prefix}/train/")),
        unload_features_sql([holdout], artifacts.uri(f"{prefix}/holdout/")),
        unload_distances_sql(window, artifacts.uri(f"{prefix}/distances/")),
    )
    counts = redshift.query(
        f"select sum(case when trip_month in ({_months_sql(window)}) then 1 else 0 end) as train, "
        f"sum(case when trip_month = '{check_month(holdout)}' then 1 else 0 end) as holdout "
        f"from {FEATURES_TABLE} where trip_month in ({_months_sql([*window, holdout])})"
    )[0]
    return {
        "training_window": f"{window[0]}..{window[-1]}",
        "holdout_month": holdout,
        "prefix": prefix,
        "uri": artifacts.uri(prefix),
        "train_rows": int(counts["train"]),
        "holdout_rows": int(counts["holdout"]),
    }


def load_trips(artifacts: ArtifactBucket, prefix: str) -> pd.DataFrame:
    """An UNLOADed train/ or holdout/ prefix as one DataFrame, the inputs as int64 (Redshift's integer
    and smallint arrive as int32 and int16)."""
    df = artifacts.read_parquet(prefix)
    inputs = feature_columns()[:-1]
    return df.astype({c: "int64" for c in inputs})


# ---- trained_model: plain functions ------------------------------------------------------------------


def git_sha() -> str:
    """The commit the pipelines image was built from (runtime.yaml: GIT_SHA, baked in by the Dockerfile)."""
    return os.environ.get("GIT_SHA") or "unknown"


def fit_and_register(client, train: pd.DataFrame, distances: pd.DataFrame, *, training_window: str, holdout_month: str,
                     run_tags: dict | None = None, params: dict | None = None) -> dict:
    """Fit the given v2 on the window, log it as a pyfunc (signature, tripmodel bundled) and register a
    version carrying the tags of contracts/mlflow.yaml that are known before evaluation.

    `client` comes from MlflowTracking.client(), which has already pointed mlflow at the tracking server
    and the contract's experiment. Sets no alias."""
    from pathlib import Path

    import mlflow

    import tripmodel

    contract = mlflow_contract()
    tags = {
        "training_window": training_window,
        "holdout_month": holdout_month,
        "features_version": str(features_contract()["version"]),
        "git_sha": git_sha(),
    }
    with mlflow.start_run(run_name=f"train {training_window}") as run:
        mlflow.set_tags({**tags, **(run_tags or {})})
        model = tripmodel.LightGBMModel().fit(train, distances)  # the given v2, default hyperparameters
        mlflow.log_params({"model": "LightGBMModel", "num_rounds": model.booster.num_trees(), "train_rows": len(train),
                           "distance_pairs": len(distances), **model.params, **(params or {})})
        info = mlflow.pyfunc.log_model(name="model", python_model=model, signature=tripmodel.model_signature(),
                                       code_paths=[str(Path(tripmodel.__file__).parent)])
        # Registered with its tags in one call, so a version never exists without them.
        version = mlflow.register_model(info.model_uri, contract["registered_model"], tags=tags)
    missing = [t for t in tags if t not in client.get_model_version(contract["registered_model"], version.version).tags]
    if missing:
        raise RuntimeError(f"version {version.version} was registered without {missing}")
    return {"model_version": int(version.version), "mlflow_run_id": run.info.run_id, **tags,
            "train_rows": len(train), "distance_pairs": len(distances)}


# ---- eval_report: plain functions --------------------------------------------------------------------


def zone_boroughs(redshift: RedshiftData) -> dict[int, str]:
    """Zone id -> borough, for evaluate()'s slices."""
    rows = redshift.query(f"select location_id, borough from {ZONES_TABLE}")
    if not rows:
        raise dg.Failure(description=f"{ZONES_TABLE} is empty: materialize taxi_zones first (the slices need the boroughs)")
    return {int(r["location_id"]): r["borough"] for r in rows}


def compare_on_holdout(client, model_version: int, holdout: pd.DataFrame, boroughs: dict[int, str],
                       load_window: Callable[[], tuple[pd.DataFrame, pd.DataFrame]], latency_samples: int = 200) -> dict:
    """tripmodel.evaluate the new version and what it must beat, on the same holdout rows.

    What it must beat is the `champion` version when the alias exists (read, never moved), otherwise a v1
    BaselineModel fitted on the same window (`load_window` returns that window's trips and distances).
    Only "the alias isn't there" means baseline: if MLflow can't be read or the champion can't be
    loaded, this raises, because quietly falling back to the easier baseline would weaken the gate.

    Returns {"metrics": evaluate()'s for the new version plus champion_mae_min, "compared_against": str}."""
    import mlflow

    import tripmodel

    name = mlflow_contract()["registered_model"]
    # The new version first, through the registry as serving loads it: evaluate() times single rows
    # before its batch, and another model's batch beforehand would leave busy threads behind.
    new = tripmodel.evaluate(mlflow.pyfunc.load_model(f"models:/{name}/{model_version}").predict, holdout, boroughs, latency_samples=latency_samples)
    champion = client.get_registered_model(name).aliases.get("champion")
    if champion is not None:
        compared_against = f"champion v{champion}"
        other = tripmodel.evaluate(mlflow.pyfunc.load_model(f"models:/{name}/{champion}").predict, holdout, boroughs, latency_samples=latency_samples)
    else:
        compared_against = "v1 baseline (no champion yet)"
        train, distances = load_window()
        baseline = tripmodel.BaselineModel().fit(train, distances)
        other = tripmodel.evaluate(lambda df: baseline.predict(None, df), holdout, boroughs, latency_samples=latency_samples)
    return {"metrics": {**new, "champion_mae_min": other["mae_min"]}, "compared_against": compared_against}


def record_metrics(client, mlflow_run_id: str, model_version: int, metrics: dict[str, float]) -> list[str]:
    """Log the contract's metrics on the training run and copy the contract's metric tags onto the
    model version. Returns the contract metrics that had no value (a slice with no holdout rows)."""
    contract = mlflow_contract()
    for name in contract["metrics"]:
        if name in metrics:
            client.log_metric(mlflow_run_id, name, metrics[name])
    for name in contract["version_tags"]:
        if name in metrics:
            client.set_model_version_tag(contract["registered_model"], str(model_version), name, f"{metrics[name]:.4f}")
    return [name for name in contract["metrics"] if name not in metrics]


# ---- the gate and the alias: plain functions ---------------------------------------------------------


def gate(mae_min: float, comparison_mae_min: float, p95_latency_ms: float, mae_margin_min: float, p95_latency_budget_ms: float) -> tuple[bool, str]:
    """The promotion rule: (passed, why). Lower MAE than the comparison by more than the margin (strictly
    lower when the margin is 0; equal is a fail), and p95 latency within the budget."""
    improvement = comparison_mae_min - mae_min
    beats = improvement > mae_margin_min
    fast = p95_latency_ms <= p95_latency_budget_ms
    why = [
        f"MAE {mae_min:.4f} min vs {comparison_mae_min:.4f} (improvement {improvement:+.4f}, needs more than {mae_margin_min:g}): "
        f"{'ok' if beats else 'FAIL'}",
        f"p95 {p95_latency_ms:.2f} ms vs budget {p95_latency_budget_ms:g} ms: {'ok' if fast else 'FAIL'}",
    ]
    return beats and fast, "; ".join(why)


def set_challenger(client, model_version: int) -> str | None:
    """Point `challenger` at a version. Returns the version it pointed at before. The only alias write in
    the pipeline, and never `champion`: the approval-gated promotion workflow (S3) owns that one."""
    name = mlflow_contract()["registered_model"]
    previous = client.get_registered_model(name).aliases.get(CHALLENGER)
    client.set_registered_model_alias(name, CHALLENGER, str(model_version))
    return previous


# ---- Dagster: the assets, the gate and the job -------------------------------------------------------


def _latest(context, asset: str) -> tuple[int, dict]:
    """(event-log id, metadata) of an asset's latest materialization."""
    records = context.instance.fetch_materializations(dg.AssetKey(asset), limit=1).records
    if not records or records[0].asset_materialization is None:
        raise dg.Failure(description=f"{asset} has never been materialized: run the train_model job")
    return records[0].storage_id, {k: v.value for k, v in records[0].asset_materialization.metadata.items()}


@dg.asset(deps=["trip_features"], group_name="model", kinds={"redshift", "s3"}, pool="redshift")
def training_set(context: dg.AssetExecutionContext, config: TrainingWindow, redshift: RedshiftData, artifacts: ArtifactBucket) -> dg.MaterializeResult:
    """The training window and the holdout month of staging.trip_features, UNLOADed as Parquet to
    training/<run id>/ in the artifact bucket, with the zone-pair distance lookup of the window only."""
    window, holdout = plan_split(config.window_start, config.window_end, config.holdout_month)
    out = unload_training_set(redshift, artifacts, window, holdout, prefix=f"training/{context.run.run_id}")
    context.log.info(f"window {out['training_window']} ({out['train_rows']} rows), holdout {holdout} ({out['holdout_rows']} rows) -> {out['uri']}")
    return dg.MaterializeResult(metadata=out)


@dg.asset(deps=[training_set], group_name="model", kinds={"lightgbm", "mlflow"})
def trained_model(context: dg.AssetExecutionContext, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.MaterializeResult:
    """tripmodel's v2 (LightGBM) fitted on the window, logged to MLflow as a pyfunc and registered as a
    new version of the contract's model, tagged with its window, holdout, features version and commit.
    No alias yet: that is `challenger`'s, behind the gate."""
    _, ts = _latest(context, "training_set")
    train = load_trips(artifacts, f"{ts['prefix']}/train")
    distances = artifacts.read_parquet(f"{ts['prefix']}/distances")
    out = fit_and_register(tracking.client(), train, distances, training_window=ts["training_window"], holdout_month=ts["holdout_month"],
                           run_tags={"dagster_run_id": context.run.run_id}, params={"training_set": ts["uri"]})
    context.log.info(f"registered version {out['model_version']} (MLflow run {out['mlflow_run_id']}), trained on {out['train_rows']} rows")
    return dg.MaterializeResult(metadata={**out, "training_set": ts["prefix"]})


@dg.asset(deps=[trained_model, "taxi_zones"], group_name="model", kinds={"mlflow"})
def eval_report(context: dg.AssetExecutionContext, redshift: RedshiftData, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dg.MaterializeResult:
    """The new version against the champion (or the v1 baseline before there is one) on the same holdout
    month: MAE overall and by slice, p95 single-row latency. Logged on the training run, copied to the
    version's tags, and shown here. The beats_champion check reads this."""
    _, tm = _latest(context, "trained_model")
    prefix, version = tm["training_set"], int(tm["model_version"])
    holdout = load_trips(artifacts, f"{prefix}/holdout")
    client = tracking.client()
    report = compare_on_holdout(client, version, holdout, zone_boroughs(redshift),
                                load_window=lambda: (load_trips(artifacts, f"{prefix}/train"), artifacts.read_parquet(f"{prefix}/distances")))
    metrics = report["metrics"]
    absent = record_metrics(client, tm["mlflow_run_id"], version, metrics)
    if absent:
        context.log.warning(f"no holdout rows for {absent}: those metrics were not logged")
    context.log.info(f"version {version}: MAE {metrics['mae_min']:.4f} min vs {report['compared_against']} {metrics['champion_mae_min']:.4f}, "
                     f"p95 {metrics['p95_latency_ms']:.2f} ms")
    return dg.MaterializeResult(metadata={
        "model_version": version,
        "mlflow_run_id": tm["mlflow_run_id"],
        "training_window": tm["training_window"],
        "holdout_month": tm["holdout_month"],
        "compared_against": report["compared_against"],
        **{k: dg.MetadataValue.float(float(v)) for k, v in metrics.items() if k != "holdout_rows"},
        "holdout_rows": int(metrics["holdout_rows"]),
    })


@dg.asset_check(asset=eval_report, blocking=True,
                description="The new version's holdout MAE beats the champion's (or the v1 baseline's) by more than the margin, within the p95 latency budget.")
def beats_champion(context: dg.AssetCheckExecutionContext, config: PromotionRule) -> dg.AssetCheckResult:
    _, report = _latest(context, "eval_report")
    passed, why = gate(report["mae_min"], report["champion_mae_min"], report["p95_latency_ms"], config.mae_margin_min, config.p95_latency_budget_ms)
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"version {report['model_version']} vs {report['compared_against']}: {why}",
        metadata={
            "model_version": dg.MetadataValue.int(int(report["model_version"])),
            "compared_against": report["compared_against"],
            "mae_min": dg.MetadataValue.float(report["mae_min"]),
            "champion_mae_min": dg.MetadataValue.float(report["champion_mae_min"]),
            "mae_improvement_min": dg.MetadataValue.float(report["champion_mae_min"] - report["mae_min"]),
            "mae_margin_min": dg.MetadataValue.float(config.mae_margin_min),
            "p95_latency_ms": dg.MetadataValue.float(report["p95_latency_ms"]),
            "p95_latency_budget_ms": dg.MetadataValue.float(config.p95_latency_budget_ms),
        },
    )


def _require_gate(context, eval_storage_id: int) -> None:
    """Refuse unless the latest beats_champion evaluation passed AND judged this very eval_report.
    Inside train_model the blocking check already stops this step; this covers `challenger` being
    materialized on its own from the UI, where no check runs."""
    record = context.instance.get_latest_asset_check_evaluation_record(dg.AssetCheckKey(dg.AssetKey("eval_report"), GATE))
    evaluation = record.evaluation if record and record.status.value in ("SUCCEEDED", "FAILED") else None
    target = evaluation.target_materialization_data if evaluation else None
    if not (evaluation and evaluation.passed and target and target.storage_id == eval_storage_id):
        raise dg.Failure(description=f"{GATE} has not passed on the latest eval_report: the challenger alias stays where it is. "
                                     "Run train_model (or eval_report with its check) first.")


@dg.asset(deps=[eval_report], group_name="model", kinds={"mlflow"})
def challenger(context: dg.AssetExecutionContext, tracking: MlflowTracking) -> dg.MaterializeResult:
    """Points the `challenger` alias at the version the beats_champion gate just passed. Nothing else in
    the pipeline moves an alias, and nothing here touches `champion`."""
    storage_id, report = _latest(context, "eval_report")
    _require_gate(context, storage_id)
    version = int(report["model_version"])
    previous = set_challenger(tracking.client(), version)
    context.log.info(f"challenger -> version {version} (was {previous or 'unset'})")
    return dg.MaterializeResult(metadata={"model_version": version, "previous_challenger": str(previous or "none"),
                                          "mae_min": dg.MetadataValue.float(report["mae_min"]),
                                          "compared_against": report["compared_against"]})


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
    description="Window to challenger: UNLOAD the training set, fit and register v2, evaluate it against the champion "
                "(or the v1 baseline), and set `challenger` only if the blocking beats_champion gate passes.",
)
