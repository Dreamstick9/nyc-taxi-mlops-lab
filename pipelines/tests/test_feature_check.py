"""feature_contract: names, types and ranges of staging.trip_features against contracts/features.yaml."""

import dagster as dg
import pytest
from fakes import LOG, FakeRedshift, fake_bucket, fake_redshift

from labpipes.contracts import features_contract
from labpipes.defs import features, marts, quality, raw_trips, schema_check, staging, trip_files, zones
from labpipes.defs import feature_check as fc
from labpipes.defs.sensor import ingest_month
from labpipes.partitions import trip_months

CONTRACT = features_contract()
# The table as trip_features.sql creates it, in the catalog's spelling: (column, data_type, char length).
CATALOG = [
    ["trip_month", "character", 7],
    ["pickup_zone_id", "integer", None],
    ["dropoff_zone_id", "integer", None],
    ["pickup_hour", "smallint", None],
    ["pickup_dow", "smallint", None],
    ["duration_min", "double precision", None],
]


def table(catalog=CATALOG) -> dict[str, str]:
    return {name: fc.canonical_type(type_, length) for name, type_, length in catalog}


def result(actual=None, rows=1000, nulls=None, outside=None, unchecked=()):
    checked = [b for b in fc.bounds(CONTRACT) if b.name not in unchecked]
    zero_nulls, zero_outside = fc.range_violations({}, checked)
    return fc.contract_result(CONTRACT, table() if actual is None else actual, rows, {**zero_nulls, **(nulls or {})},
                              {**zero_outside, **(outside or {})}, list(unchecked))


def test_expected_columns_come_from_the_contract_not_from_code():
    assert fc.expected_columns(CONTRACT) == {
        "trip_month": "char(7)", "pickup_zone_id": "integer", "dropoff_zone_id": "integer",
        "pickup_hour": "smallint", "pickup_dow": "smallint", "duration_min": "double precision",
    }
    assert "est_distance_miles" not in fc.expected_columns(CONTRACT)  # derived: not a table column
    changed = {**CONTRACT, "inputs": [*CONTRACT["inputs"], {"name": "passenger_count", "type": "smallint", "range": [0, 9]}]}
    assert fc.expected_columns(changed)["passenger_count"] == "smallint"
    assert any("missing column 'passenger_count'" in d for d in fc.column_differences(table(), changed))


@pytest.mark.parametrize("spelling, canonical", [
    ("character(7)", "char(7)"), ("CHAR(7)", "char(7)"), ("bpchar(7)", "char(7)"), ("char( 7 )", "char(7)"),
    ("int2", "smallint"), ("int4", "integer"), ("int", "integer"), ("INTEGER", "integer"),
    ("float8", "double precision"), ("double  precision", "double precision"), ("float", "double precision"),
    ("character varying(256)", "varchar(256)"), ("int8", "bigint"), ("float4", "real"), ("numeric(18,2)", "numeric(18,2)"),
])
def test_redshift_spellings_of_one_type_compare_equal(spelling, canonical):
    assert fc.canonical_type(spelling) == canonical


def test_catalog_length_completes_a_character_type_only():
    assert fc.canonical_type("character", 7) == "char(7)"
    assert fc.canonical_type("character varying", 256) == "varchar(256)"
    assert fc.canonical_type("character(7)", 7) == "char(7)"
    assert fc.canonical_type("integer", 32) == "integer"
    assert fc.canonical_type("character", 8) != fc.canonical_type("char(7)")


def test_the_given_table_matches_the_contract():
    assert fc.column_differences(table(), CONTRACT) == []
    r = result()
    assert r.passed and r.severity == dg.AssetCheckSeverity.ERROR
    assert r.metadata["differences"].value == [] and r.metadata["rows"].value == 1000


def test_column_order_is_not_part_of_the_contract():
    assert fc.column_differences(table(CATALOG[::-1]), CONTRACT) == []


def test_missing_column_is_a_mismatch():
    found = fc.column_differences(table([c for c in CATALOG if c[0] != "pickup_dow"]), CONTRACT)
    assert found == ["missing column 'pickup_dow' (smallint)"]
    assert not result(table([c for c in CATALOG if c[0] != "pickup_dow"]), unchecked=["pickup_dow"]).passed


def test_missing_table_is_every_column_missing():
    assert len(fc.column_differences({}, CONTRACT)) == len(CATALOG)


def test_extra_column_is_a_mismatch():
    found = fc.column_differences(table([*CATALOG, ["passenger_count", "smallint", None]]), CONTRACT)
    assert found == ["extra column 'passenger_count' (smallint) is not in the contract"]
    derived = fc.column_differences(table([*CATALOG, ["est_distance_miles", "double precision", None]]), CONTRACT)
    assert len(derived) == 1 and "extra column 'est_distance_miles'" in derived[0]


@pytest.mark.parametrize("leak", CONTRACT["forbidden"])
def test_forbidden_post_trip_column_is_a_mismatch(leak):
    actual = table([*CATALOG, [leak, "double precision", None]])
    found = fc.column_differences(actual, CONTRACT)
    assert len(found) == 1 and found[0].startswith(f"forbidden column {leak!r}")
    r = result(actual)
    assert not r.passed and leak in r.description


def test_wrong_type_is_a_mismatch():
    wrong = [["pickup_hour", "integer", None] if c[0] == "pickup_hour" else c for c in CATALOG]
    assert fc.column_differences(table(wrong), CONTRACT) == ["column 'pickup_hour': type integer, expected smallint"]
    longer = [["trip_month", "character", 10], *CATALOG[1:]]
    assert fc.column_differences(table(longer), CONTRACT) == ["column 'trip_month': type char(10), expected char(7)"]
    real = [*CATALOG[:-1], ["duration_min", "real", None]]
    assert fc.column_differences(table(real), CONTRACT) == ["column 'duration_min': type real, expected double precision"]


def test_bounds_are_the_contracts_and_the_label_floor_is_exclusive():
    by = {b.name: b for b in fc.bounds(CONTRACT)}
    assert list(by) == ["pickup_zone_id", "dropoff_zone_id", "pickup_hour", "pickup_dow", "duration_min"]
    assert by["pickup_zone_id"].outside() == "pickup_zone_id < 1 or pickup_zone_id > 263"
    assert by["pickup_hour"].outside() == "pickup_hour < 0 or pickup_hour > 23"
    assert by["pickup_dow"].outside() == "pickup_dow < 0 or pickup_dow > 6"
    assert by["duration_min"].outside() == "duration_min <= 0 or duration_min > 180"
    assert by["duration_min"].interval() == "(0, 180]" and by["pickup_dow"].interval() == "[0, 6]"


def test_range_sql_counts_nulls_and_out_of_range_rows_for_one_month():
    sql = fc.range_sql(CONTRACT, "2024-03", fc.bounds(CONTRACT))
    assert sql.startswith("select count(*) as rows, ")
    assert sql.endswith("from staging.trip_features where trip_month = '2024-03'")
    assert "coalesce(sum(case when duration_min is null then 1 else 0 end), 0) as duration_min__null" in sql
    assert "coalesce(sum(case when duration_min <= 0 or duration_min > 180 then 1 else 0 end), 0) as duration_min__out" in sql
    assert "coalesce(sum(case when pickup_dow < 0 or pickup_dow > 6 then 1 else 0 end), 0) as pickup_dow__out" in sql


def test_nothing_unvalidated_reaches_sql():
    with pytest.raises(ValueError):
        fc.range_sql(CONTRACT, "2024-03'; drop table x; --", fc.bounds(CONTRACT))
    with pytest.raises(ValueError):
        fc.bounds({**CONTRACT, "inputs": [{"name": "x; drop table y", "type": "integer", "range": [0, 1]}]})
    with pytest.raises(ValueError):
        fc.bounds({**CONTRACT, "inputs": [{"name": "x", "type": "integer", "range": [0, "1 or 1=1"]}]})
    with pytest.raises(ValueError):
        fc.columns_sql({**CONTRACT, "table": "staging.trip_features' or '1'='1"})


def test_out_of_range_value_fails_and_says_where():
    r = result(outside={"pickup_zone_id": 12})
    assert not r.passed and "column 'pickup_zone_id': 12 row(s) outside [1, 263]" in r.description
    assert r.metadata["out_of_range_rows"].value["pickup_zone_id"] == 12
    assert r.metadata["out_of_range_rows"].value["duration_min"] == 0
    zero = result(outside={"duration_min": 1})  # a 0-minute trip: the floor is exclusive
    assert not zero.passed and "outside (0, 180]" in zero.description


def test_one_bad_row_is_enough():
    assert not result(rows=3_000_000, outside={"pickup_hour": 1}).passed
    assert not result(rows=3_000_000, nulls={"pickup_hour": 1}).passed


def test_null_fails_and_says_where():
    r = result(nulls={"duration_min": 4})
    assert not r.passed and "column 'duration_min': 4 NULL row(s)" in r.description
    assert r.metadata["null_rows"].value == {"pickup_zone_id": 0, "dropoff_zone_id": 0, "pickup_hour": 0, "pickup_dow": 0, "duration_min": 4}


def test_range_violations_reads_the_query_row():
    columns = fc.bounds(CONTRACT)
    row = {"rows": 10, "pickup_dow__null": 2, "pickup_dow__out": 0, "duration_min__out": 3}
    nulls, outside = fc.range_violations(row, columns)
    assert nulls["pickup_dow"] == 2 and outside["duration_min"] == 3
    assert set(nulls) == set(outside) == {b.name for b in columns}


def test_feature_contract_is_a_blocking_partitioned_check_on_trip_features():
    (spec,) = fc.feature_contract.check_specs
    assert spec.name == "feature_contract" and spec.blocking and spec.asset_key == dg.AssetKey("trip_features")
    assert spec.partitions_def == trip_months


# The check inside Dagster, against a fake Redshift that answers the catalog and the range query.


class FeatureRedshift(FakeRedshift):
    """catalog: svv_columns rows as [name, data_type, length]; counts: overrides for the range query's row."""

    catalog: list = CATALOG
    counts: dict = {}

    def query(self, sql: str) -> list[dict]:
        if "from svv_columns" in sql:
            LOG.append(sql)
            return [{"column_name": n, "data_type": t, "character_maximum_length": length} for n, t, length in self.catalog]
        if "__null" in sql:
            LOG.append(sql)
            return [{"rows": self.rows, **self.counts}]
        return super().query(sql)


@dg.asset(partitions_def=trip_months, deps=["trip_features"])
def after_features() -> None:
    """Stands in for whatever a run puts downstream of trip_features."""


def run_check(**fake):
    fake_redshift()  # clears LOG
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("trip_months", ["2024-03"])
    rs = FeatureRedshift(cluster_id="lab", database="lab", db_user="labadmin", rows=42, **fake)
    run = dg.materialize([features.trip_features, fc.feature_contract, after_features], partition_key="2024-03",
                         instance=instance, resources={"redshift": rs}, raise_on_error=False)
    (evaluation,) = run.get_asset_check_evaluations()
    downstream_ran = bool(run.asset_materializations_for_node("after_features"))
    return run, evaluation, downstream_ran


def test_check_passes_on_the_given_table_and_queries_only_its_month():
    run, evaluation, downstream_ran = run_check()
    assert run.success and evaluation.passed and downstream_ran
    assert evaluation.check_name == "feature_contract" and evaluation.metadata["rows"].value == 42
    catalog_sql = next(s for s in LOG if "from svv_columns" in s)
    assert "table_schema = 'staging' and table_name = 'trip_features'" in catalog_sql
    range_sql = next(s for s in LOG if "__null" in s)
    assert range_sql == fc.range_sql(CONTRACT, "2024-03", fc.bounds(CONTRACT))


def test_out_of_range_rows_block_what_is_downstream():
    run, evaluation, downstream_ran = run_check(counts={"pickup_zone_id__out": 7})
    assert not run.success and not evaluation.passed and not downstream_ran
    assert evaluation.severity == dg.AssetCheckSeverity.ERROR
    assert evaluation.metadata["out_of_range_rows"].value["pickup_zone_id"] == 7


def test_null_rows_block_what_is_downstream():
    run, evaluation, downstream_ran = run_check(counts={"duration_min__null": 1})
    assert not evaluation.passed and not downstream_ran
    assert evaluation.metadata["null_rows"].value["duration_min"] == 1


def test_leaky_column_blocks_what_is_downstream():  # the colleague's patch: trip_distance as an input
    run, evaluation, downstream_ran = run_check(catalog=[*CATALOG, ["trip_distance", "double precision", None]])
    assert not run.success and not evaluation.passed and not downstream_ran
    assert any("forbidden column 'trip_distance'" in d for d in evaluation.metadata["differences"].value)


def test_mismatched_columns_fail_without_being_queried():
    wrong = [["pickup_hour", "character varying", 8] if c[0] == "pickup_hour" else c for c in CATALOG if c[0] != "pickup_dow"]
    run, evaluation, downstream_ran = run_check(catalog=wrong)
    assert not evaluation.passed and not downstream_ran
    assert evaluation.metadata["ranges_not_checked"].value == ["pickup_hour", "pickup_dow"]
    range_sql = next(s for s in LOG if "__null" in s)
    assert "pickup_hour" not in range_sql and "pickup_dow" not in range_sql and "duration_min__out" in range_sql


def test_missing_table_fails_without_a_range_query():
    run, evaluation, downstream_ran = run_check(catalog=[])
    assert not evaluation.passed and not downstream_ran
    assert len(evaluation.metadata["differences"].value) == len(CATALOG)
    assert not any("__null" in s for s in LOG)


def test_ingest_job_builds_trip_features_and_runs_its_gate():
    modules = [zones, trip_files, raw_trips, staging, marts, features, schema_check, quality, fc]
    defs = dg.Definitions(
        assets=dg.load_assets_from_modules(modules), asset_checks=dg.load_asset_checks_from_modules(modules), jobs=[ingest_month],
        resources={"redshift": fake_redshift(), "data_bucket": fake_bucket({})},
    )
    graph = defs.resolve_asset_graph()
    assets = {k.to_user_string() for k in ingest_month.selection.resolve(graph)}
    assert assets == {"taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats", "trip_features"}
    checks = {k.to_user_string() for k in ingest_month.selection.resolve_checks(graph)}
    assert {"trip_files:schema_contract", "trip_features:feature_contract"} <= checks
    assert defs.resolve_job_def("ingest_month").name == "ingest_month"
