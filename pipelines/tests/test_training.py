"""S2 training: the window and holdout SQL, the registered version's tags and metrics, the beats_champion
gate, and the challenger alias. Fake Redshift (its UNLOADs write synthetic trips to a local folder) and
a local MLflow on SQLite; nothing here calls AWS."""

from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import fakes
import mlflow
import pandas as pd
import pytest
from fakes import LocalArtifacts, synthetic_trips, unloading_redshift
from mlflow import MlflowClient

from labpipes.contracts import mlflow_contract
from labpipes.defs import training
from labpipes.defs.training import (
    PromotionRule,
    TrainingWindow,
    beats_champion,
    challenger,
    eval_report,
    gate,
    plan_split,
    set_challenger,
    train_model,
    trained_model,
    training_set,
    unload_distances_sql,
    unload_features_sql,
    window_months,
)
from labpipes.resources import MlflowTracking

MODEL = "trip-duration"
WINDOW, HOLDOUT = ["2024-01", "2024-02", "2024-03"], "2024-04"
GATE_OP = "eval_report_beats_champion"  # the op Dagster builds for the check: where its run config goes


@dataclass
class Lab:
    root: str
    resources: dict
    client: MlflowClient
    instance: dg.DagsterInstance

    def run(self, run_config: dict | None = None) -> dg.ExecuteInProcessResult:
        """The real train_model job, in process, on the fakes."""
        defs = dg.Definitions(assets=[training_set, trained_model, eval_report, challenger], asset_checks=[beats_champion],
                              jobs=[train_model], resources=self.resources)
        return defs.resolve_job_def("train_model").execute_in_process(instance=self.instance, run_config=run_config, raise_on_error=False)

    def aliases(self) -> dict[str, int]:
        return {k: int(v) for k, v in self.client.get_registered_model(MODEL).aliases.items()}

    def metadata(self, asset: str) -> dict:
        event = self.instance.get_latest_materialization_event(dg.AssetKey(asset))
        return {k: v.value for k, v in event.asset_materialization.metadata.items()}


def make_lab(root, instance) -> Lab:
    uri = f"sqlite:///{root}/mlflow.db"
    mlflow.set_tracking_uri(uri)
    mlflow.create_experiment(mlflow_contract()["experiment"], artifact_location=str(root / "mlflow-artifacts"))
    resources = {"redshift": unloading_redshift(str(root)), "artifacts": LocalArtifacts(bucket="artifacts", root=str(root)),
                 "tracking": MlflowTracking(tracking_uri=uri)}
    return Lab(str(root), resources, MlflowClient(uri), instance)


@pytest.fixture
def lab(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "abc1234")
    with dg.instance_for_test() as instance:
        yield make_lab(tmp_path, instance)


@pytest.fixture(scope="module")
def first_run(tmp_path_factory):
    """One train_model run with no config and no champion, shared by the tests that only read it."""
    with pytest.MonkeyPatch.context() as mp, dg.instance_for_test() as instance:
        mp.setenv("GIT_SHA", "abc1234")
        lab = make_lab(tmp_path_factory.mktemp("first_run"), instance)
        result = lab.run()
        yield lab, result, list(fakes.LOG)


# ---- the decisions, the window and the SQL -----------------------------------------------------------


def test_defaults_are_the_recorded_decisions():
    """A change here is a change to a graded decision (decisions/), not a refactor."""
    assert plan_split(**TrainingWindow().model_dump()) == (WINDOW, HOLDOUT)
    assert PromotionRule().model_dump() == {"mae_margin_min": 0.0, "p95_latency_budget_ms": 20.0}


def test_window_months_and_split_validation():
    assert window_months("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]
    for bad in [("2024-03", "2024-01", "2024-04"),  # window ends before it starts
                ("2024-01", "2024-03", "2024-03"),  # holdout inside the window
                ("2024-02", "2024-03", "2024-01"),  # holdout before the window
                ("2024-01", "2024-03", "2024-04'') --")]:  # not a month: never reaches SQL
        with pytest.raises(ValueError):
            plan_split(*bad)
    with pytest.raises(ValueError):
        unload_features_sql(["2024-01' or 1=1 --"], "s3://artifacts/training/x/train/")
    with pytest.raises(ValueError):
        unload_features_sql(WINDOW, "s3://artifacts/x') to 's3://elsewhere/")


def test_unload_sql_has_the_right_months_and_the_lookup_excludes_the_holdout():
    train = unload_features_sql(WINDOW, "s3://artifacts/training/r/train/")
    holdout = unload_features_sql([HOLDOUT], "s3://artifacts/training/r/holdout/")
    distances = unload_distances_sql(WINDOW, "s3://artifacts/training/r/distances/")
    assert "from staging.trip_features where trip_month in (''2024-01'', ''2024-02'', ''2024-03'')" in train and "2024-04" not in train
    assert "where trip_month in (''2024-04'')" in holdout and not any(m in holdout for m in WINDOW)
    assert "trip_distance" not in train and "trip_distance" not in holdout  # forbidden as a model input
    assert "median(trip_distance) as est_distance_miles from staging.stg_trips" in distances
    assert all(f"''{m}''" in distances for m in WINDOW) and HOLDOUT not in distances
    for sql in (train, holdout, distances):
        assert sql.startswith("unload (") and sql.endswith("iam_role default format as parquet")


def test_training_set_unloads_window_holdout_and_window_only_distances(first_run):
    lab, result, log = first_run
    assert result.success
    md = lab.metadata("training_set")
    assert (md["training_window"], md["holdout_month"]) == ("2024-01..2024-03", HOLDOUT)
    unloads = [s for s in log if s.startswith("unload")]
    assert len(unloads) == 3 and all(f"to 's3://artifacts/{md['prefix']}/" in s for s in unloads)
    assert [HOLDOUT in s for s in unloads] == [False, True, False]  # only the holdout UNLOAD names the holdout month
    artifacts = lab.resources["artifacts"]
    assert len(artifacts.read_parquet(f"{md['prefix']}/train")) == 12000 and len(artifacts.read_parquet(f"{md['prefix']}/holdout")) == 4000
    got = artifacts.read_parquet(f"{md['prefix']}/distances").set_index(["pickup_zone_id", "dropoff_zone_id"])["est_distance_miles"].sort_index()
    trips = synthetic_trips([*WINDOW, HOLDOUT])
    pair_median = lambda df: df.groupby(["pickup_zone_id", "dropoff_zone_id"])["trip_distance"].median().sort_index()  # noqa: E731
    pd.testing.assert_series_equal(got, pair_median(trips[trips.trip_month.isin(WINDOW)]), check_names=False)
    assert not got.equals(pair_median(trips))  # with the holdout month in, the lookup would differ


def test_training_set_refuses_a_month_without_features(lab):
    result = lab.run({"ops": {"training_set": {"config": {"window_start": "2024-02", "window_end": "2024-04", "holdout_month": "2024-05"}}}})
    assert not result.success
    assert not [s for s in fakes.LOG if s.startswith("unload")]
    assert not lab.client.search_registered_models()


# ---- the registered version --------------------------------------------------------------------------


def test_version_carries_every_contract_tag_and_metric(first_run):
    lab, result, _ = first_run
    contract = mlflow_contract()
    (mv,) = lab.client.search_model_versions(f"name='{MODEL}'")
    assert set(contract["version_tags"]) <= set(mv.tags)
    assert (mv.tags["training_window"], mv.tags["holdout_month"]) == ("2024-01..2024-03", HOLDOUT)
    assert (mv.tags["features_version"], mv.tags["git_sha"]) == ("1", "abc1234")
    run = lab.client.get_run(mv.run_id)
    assert run.info.experiment_id == lab.client.get_experiment_by_name(contract["experiment"]).experiment_id
    assert set(contract["metrics"]) <= set(run.data.metrics)
    for name in ("mae_min", "p95_latency_ms", "champion_mae_min"):  # the tags are the run's metrics
        assert float(mv.tags[name]) == pytest.approx(run.data.metrics[name], abs=1e-4)
    report = lab.metadata("eval_report")  # and Dagster shows the same numbers
    assert report["model_version"] == int(mv.version) and report["compared_against"].startswith("v1 baseline")
    assert all(report[name] == pytest.approx(run.data.metrics[name]) for name in contract["metrics"])


def test_registered_model_is_a_pyfunc_that_fills_the_distance(first_run):
    lab, _, _ = first_run
    mlflow.set_tracking_uri(lab.resources["tracking"].tracking_uri)
    loaded = mlflow.pyfunc.load_model(f"models:/{MODEL}@challenger")
    assert loaded.metadata.signature.inputs.input_names() == ["pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "est_distance_miles"]
    row = pd.DataFrame({"pickup_zone_id": [3], "dropoff_zone_id": [9], "pickup_hour": [8], "pickup_dow": [2]}).astype("int64")
    assert loaded.predict(row)[0] > 0


# ---- the gate ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mae", "comparison", "p95", "passed"),
    [
        (4.0, 4.5, 5.0, True),       # better and fast
        (4.4999, 4.5, 20.0, True),   # any improvement counts (margin 0); exactly on the budget is within it
        (4.5, 4.5, 5.0, False),      # equal is not better
        (4.6, 4.5, 5.0, False),      # worse
        (4.0, 4.5, 20.01, False),    # better but over the latency budget
        (4.6, 4.5, 25.0, False),     # both
    ],
)
def test_gate_rule_with_the_decided_values(mae, comparison, p95, passed):
    rule = PromotionRule()
    assert gate(mae, comparison, p95, rule.mae_margin_min, rule.p95_latency_budget_ms)[0] is passed


def test_gate_margin_means_more_than_the_margin():
    assert gate(4.0, 4.5, 5.0, mae_margin_min=0.4, p95_latency_budget_ms=20.0)[0]
    assert not gate(4.0, 4.5, 5.0, mae_margin_min=0.5, p95_latency_budget_ms=20.0)[0]


def check_on(mae: float, comparison: float, p95: float) -> dg.AssetCheckResult:
    """The Dagster check itself, on an eval_report materialization with these numbers."""
    with dg.instance_for_test() as instance:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key="eval_report", metadata={
            "model_version": 2, "compared_against": "champion v1", "mae_min": mae, "champion_mae_min": comparison, "p95_latency_ms": p95}))
        return beats_champion(dg.build_asset_check_context(instance=instance), config=PromotionRule())


def test_check_is_blocking_and_reports_both_maes_the_margin_and_the_latency():
    (spec,) = beats_champion.check_specs
    assert spec.blocking and spec.key == dg.AssetCheckKey(dg.AssetKey("eval_report"), "beats_champion")
    assert "beats_champion" in mlflow_contract()["pipeline"]["blocking_checks"]["eval_report"]
    result = check_on(4.0, 4.5, 5.0)
    assert result.passed and result.severity == dg.AssetCheckSeverity.ERROR
    shown = {k: v.value for k, v in result.metadata.items()}
    assert shown == {"model_version": 2, "compared_against": "champion v1", "mae_min": 4.0, "champion_mae_min": 4.5,
                     "mae_improvement_min": 0.5, "mae_margin_min": 0.0, "p95_latency_ms": 5.0, "p95_latency_budget_ms": 20.0}


def test_check_fails_on_equal_or_worse_mae_and_over_the_latency_budget():
    assert not check_on(4.5, 4.5, 5.0).passed
    assert not check_on(4.6, 4.5, 5.0).passed
    assert not check_on(4.0, 4.5, 20.5).passed


# ---- the alias ---------------------------------------------------------------------------------------


def test_passing_run_sets_challenger_and_only_challenger(first_run):
    lab, result, _ = first_run
    (evaluation,) = result.get_asset_check_evaluations()
    assert result.success and evaluation.passed and evaluation.check_name == "beats_champion"
    assert lab.aliases() == {"challenger": 1}  # no champion: that alias is the promotion workflow's
    assert lab.metadata("challenger")["model_version"] == 1


def test_failed_gate_stops_challenger_in_the_run_and_on_its_own(lab):
    assert lab.run().success and lab.aliases() == {"challenger": 1}
    # A budget no model meets: version 2 is registered and evaluated, but the gate blocks the alias.
    failed = lab.run({"ops": {GATE_OP: {"config": {"p95_latency_budget_ms": 0.0}}}})
    (evaluation,) = failed.get_asset_check_evaluations()
    assert not failed.success and not evaluation.passed
    assert {e.step_key for e in failed.all_events if e.event_type_value == "STEP_SUCCESS"} == {"training_set", "trained_model", "eval_report"}
    assert lab.metadata("eval_report")["model_version"] == 2 and lab.aliases() == {"challenger": 1}
    # Materializing challenger alone (no check in that run) doesn't get around the failed gate.
    alone = dg.materialize([challenger], instance=lab.instance, resources=lab.resources, raise_on_error=False)
    assert not alone.success and lab.aliases() == {"challenger": 1}


def test_champion_is_what_a_new_version_is_compared_with_and_is_never_moved(lab):
    assert lab.run().success
    v1_mae = float(lab.client.get_model_version(MODEL, "1").tags["mae_min"])
    lab.client.set_registered_model_alias(MODEL, "champion", "1")  # what S3's promotion workflow does; never the pipeline
    lab.run()
    report = lab.metadata("eval_report")
    assert report["model_version"] == 2 and report["compared_against"] == "champion v1"
    assert report["champion_mae_min"] == pytest.approx(v1_mae, abs=1e-4)  # the champion on the same holdout, not the baseline
    assert float(lab.client.get_model_version(MODEL, "2").tags["champion_mae_min"]) == pytest.approx(v1_mae, abs=1e-4)
    assert lab.aliases()["champion"] == 1 and set(lab.aliases()) == {"champion", "challenger"}


def test_the_only_alias_write_is_challenger():
    class Spy:
        def __init__(self):
            self.calls = []

        def get_registered_model(self, name):
            return type("RegisteredModel", (), {"aliases": {"champion": "3", "challenger": "4"}})()

        def set_registered_model_alias(self, name, alias, version):
            self.calls.append((name, alias, version))

    spy = Spy()
    assert set_challenger(spy, 5) == "4"
    assert spy.calls == [(MODEL, "challenger", "5")]
    source = Path(training.__file__).read_text()
    assert source.count("set_registered_model_alias(") == 1 and "delete_registered_model_alias" not in source
    assert training.CHALLENGER == "challenger"


# ---- the job -----------------------------------------------------------------------------------------


def test_job_is_train_model_with_the_contract_assets_and_the_gate(monkeypatch):
    for k in ("REDSHIFT_CLUSTER_ID", "REDSHIFT_DATABASE", "REDSHIFT_DB_USER", "LAB_DATA_BUCKET", "LAB_ARTIFACT_BUCKET", "MLFLOW_TRACKING_URI", "LLM_HOST"):
        monkeypatch.setenv(k, "x")
    from labpipes.definitions import defs

    assert train_model.name == "train_model"
    job = defs.resolve_job_def("train_model")
    selected = {k.to_user_string() for k in job.asset_layer.selected_asset_keys}
    assert selected == set(mlflow_contract()["pipeline"]["assets"]) - {"trip_features"}
    assert {k.name for k in job.asset_layer.asset_graph.asset_check_keys} >= {"beats_champion"}
    assert GATE_OP in {n.name for n in job.graph.nodes}
    graph = defs.resolve_asset_graph()
    parents = {a: {p.to_user_string() for p in graph.get(dg.AssetKey(a)).parent_keys} for a in selected}
    assert parents["training_set"] == {"trip_features"} and parents["trained_model"] == {"training_set"}
    assert "trained_model" in parents["eval_report"] and parents["challenger"] == {"eval_report"}
    assert {graph.get(dg.AssetKey(a)).group_name for a in selected} == {"model"}
