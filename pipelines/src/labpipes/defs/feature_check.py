"""S2: the feature_contract check on trip_features.

contracts/features.yaml lists the model's inputs, label and the post-trip columns that must never be
inputs; contracts/mlflow.yaml says this check blocks. "Matches" here means names, types and ranges:

- Names: the table's columns are exactly the contract's keys + inputs + label. A missing column, an
  extra one, or a `forbidden` (post-trip) one is a mismatch. `derived` inputs are not table columns.
- Types: each column's Redshift type equals the contract's, after normalising Redshift's spellings
  (character(7) = char(7), int4 = integer, float8 = double precision, ...).
- Ranges: in this partition's month, no input or label is NULL or outside the contract's range. The
  label's lower bound is exclusive (`exclusive_min: true`).

The column list, the types and the bounds are read from the contract, never written here. Names and
bounds are validated before they are formatted into SQL (batch statements take no parameters).
Gameday E1 will test it.
"""

import math
import re
from dataclasses import dataclass

import dagster as dg

from ..contracts import features_contract
from ..partitions import check_month, trip_months
from ..resources import RedshiftData

IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
TYPE = re.compile(r"\s*([a-z0-9_ ]+?)\s*(?:\(\s*([0-9 ,]+?)\s*\))?\s*")

# Redshift's names for one type, mapped to a single spelling.
TYPE_NAMES = {
    "char": "char", "character": "char", "nchar": "char", "bpchar": "char",
    "varchar": "varchar", "character varying": "varchar", "nvarchar": "varchar", "text": "varchar",
    "smallint": "smallint", "int2": "smallint",
    "integer": "integer", "int": "integer", "int4": "integer",
    "bigint": "bigint", "int8": "bigint",
    "real": "real", "float4": "real",
    "double precision": "double precision", "float8": "double precision", "float": "double precision",
    "numeric": "numeric", "decimal": "numeric",
    "boolean": "boolean", "bool": "boolean",
}
SIZED = {"char", "varchar"}  # the catalog reports their length in a separate column


def identifier(name: str) -> str:
    """A contract name that is safe inside SQL text: lower-case letters, digits and underscores."""
    if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
        raise ValueError(f"not a plain SQL identifier: {name!r}")
    return name


def number(value) -> str:
    """A contract bound as SQL text; anything that isn't a finite number never reaches the query."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"not a numeric bound: {value!r}")
    return repr(value)


def canonical_type(text: str, length: int | None = None) -> str:
    """One spelling per Redshift type: 'character(7)', 'CHAR(7)' and ('character', 7) are all 'char(7)'."""
    m = TYPE.fullmatch(str(text).lower())
    if not m:
        return str(text).strip().lower()
    base = " ".join(m[1].split())
    name = TYPE_NAMES.get(base, base)
    size = re.sub(r"\s+", "", m[2]) if m[2] else (str(int(length)) if length is not None and name in SIZED else "")
    return f"{name}({size})" if size else name


def expected_columns(contract: dict) -> dict[str, str]:
    """Column name -> type for what the table must hold: keys, then inputs, then the label."""
    columns = [*contract.get("keys", []), *contract["inputs"], contract["label"]]
    return {identifier(c["name"]): canonical_type(c["type"]) for c in columns}


def column_differences(actual: dict[str, str], contract: dict) -> list[str]:
    """Human-readable name and type mismatches between the table's columns and the contract ([] if none).

    `actual` is column name -> canonical type, as table_columns() returns it."""
    expected = expected_columns(contract)
    forbidden = set(contract.get("forbidden", []))
    found = []
    for name, type_ in expected.items():
        if name not in actual:
            found.append(f"missing column {name!r} ({type_})")
        elif actual[name] != type_:
            found.append(f"column {name!r}: type {actual[name]}, expected {type_}")
    for name, type_ in actual.items():
        if name in expected:
            continue
        if name in forbidden:
            found.append(f"forbidden column {name!r} ({type_}): known only after the trip ends")
        else:
            found.append(f"extra column {name!r} ({type_}) is not in the contract")
    return found


@dataclass(frozen=True)
class Bound:
    """One input or the label: never NULL, and inside its range when the contract gives one."""

    name: str
    low: float | None = None
    high: float | None = None
    exclusive_min: bool = False
    exclusive_max: bool = False

    def outside(self) -> str:
        """The SQL predicate for a value outside the range ('' when the contract gives no range)."""
        parts = []
        if self.low is not None:
            parts.append(f"{self.name} {'<=' if self.exclusive_min else '<'} {number(self.low)}")
        if self.high is not None:
            parts.append(f"{self.name} {'>=' if self.exclusive_max else '>'} {number(self.high)}")
        return " or ".join(parts)

    def interval(self) -> str:
        if self.low is None and self.high is None:
            return "any value"
        return f"{'(' if self.exclusive_min else '['}{self.low}, {self.high}{')' if self.exclusive_max else ']'}"


def bounds(contract: dict) -> list[Bound]:
    """The contract's inputs and label, with their ranges."""
    found = []
    for c in [*contract["inputs"], contract["label"]]:
        low, high = c["range"] if c.get("range") is not None else (None, None)
        bound = Bound(identifier(c["name"]), low, high, bool(c.get("exclusive_min", False)), bool(c.get("exclusive_max", False)))
        bound.outside()  # validates the bounds now, not when the query is built
        found.append(bound)
    return found


def table_name(contract: dict) -> tuple[str, str]:
    """(schema, table) of the contract's table, validated."""
    schema, _, table = str(contract["table"]).partition(".")
    return identifier(schema), identifier(table)


def columns_sql(contract: dict) -> str:
    """The table's columns as Redshift's catalog describes them."""
    schema, table = table_name(contract)
    return (
        "select column_name, data_type, character_maximum_length from svv_columns "
        f"where table_schema = '{schema}' and table_name = '{table}' order by ordinal_position"
    )


def table_columns(redshift: RedshiftData, contract: dict) -> dict[str, str]:
    """Column name -> canonical type of the contract's table ({} if the table doesn't exist)."""
    rows = redshift.query(columns_sql(contract))
    return {str(r["column_name"]).lower(): canonical_type(r["data_type"], r.get("character_maximum_length")) for r in rows}


def range_sql(contract: dict, month: str, columns: list[Bound]) -> str:
    """One row for a month: its row count, and per column the NULL rows and the rows outside the range."""
    schema, table = table_name(contract)
    counts = ["count(*) as rows"]
    for b in columns:
        counts.append(f"coalesce(sum(case when {b.name} is null then 1 else 0 end), 0) as {b.name}__null")
        if b.outside():
            counts.append(f"coalesce(sum(case when {b.outside()} then 1 else 0 end), 0) as {b.name}__out")
    return f"select {', '.join(counts)} from {schema}.{table} where trip_month = '{check_month(month)}'"


def range_violations(row: dict, columns: list[Bound]) -> tuple[dict[str, int], dict[str, int]]:
    """(NULL rows, out-of-range rows) per column, from range_sql's row. Every checked column is listed."""
    nulls = {b.name: int(row.get(f"{b.name}__null") or 0) for b in columns}
    outside = {b.name: int(row.get(f"{b.name}__out") or 0) for b in columns if b.outside()}
    return nulls, outside


def measure_ranges(redshift: RedshiftData, contract: dict, month: str, columns: list[Bound]) -> tuple[int, dict[str, int], dict[str, int]]:
    """(rows, NULL rows per column, out-of-range rows per column) for one month."""
    if not columns:
        return 0, {}, {}
    row = redshift.query(range_sql(contract, month, columns))[0]
    return int(row["rows"]), *range_violations(row, columns)


def contract_result(contract: dict, actual: dict[str, str], rows: int, nulls: dict[str, int], outside: dict[str, int],
                    unchecked: list[str]) -> dg.AssetCheckResult:
    """Pass only when names and types match and no checked column has a NULL or out-of-range row."""
    intervals = {b.name: b.interval() for b in bounds(contract)}
    differences = column_differences(actual, contract)
    found = list(differences)
    found += [f"column {name!r}: {n} NULL row(s)" for name, n in nulls.items() if n]
    found += [f"column {name!r}: {n} row(s) outside {intervals[name]}" for name, n in outside.items() if n]
    return dg.AssetCheckResult(
        passed=not found and not unchecked,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"matches the contract ({rows} rows)" if not found
        else "; ".join(found[:5]) + (f" (+{len(found) - 5} more)" if len(found) > 5 else ""),
        metadata={
            "differences": dg.MetadataValue.json(differences),
            "columns": dg.MetadataValue.json(actual),
            "expected_columns": dg.MetadataValue.json(expected_columns(contract)),
            "rows": dg.MetadataValue.int(rows),
            "null_rows": dg.MetadataValue.json(nulls),
            "out_of_range_rows": dg.MetadataValue.json(outside),
            "ranges": dg.MetadataValue.json(intervals),
            "ranges_not_checked": dg.MetadataValue.json(unchecked),
            "features_version": dg.MetadataValue.text(str(contract.get("version"))),
        },
    )


@dg.asset_check(asset="trip_features", name="feature_contract", partitions_def=trip_months, blocking=True,
                description="staging.trip_features matches contracts/features.yaml: column names, types and value ranges.")
def feature_contract(context: dg.AssetCheckExecutionContext, redshift: RedshiftData) -> dg.AssetCheckResult:
    month = check_month(context.partition_key)
    contract = features_contract()
    expected = expected_columns(contract)
    actual = table_columns(redshift, contract)
    # A column that is missing or has the wrong type is already a failure; its values aren't queried,
    # so the check reports the mismatch instead of dying on a SQL error.
    checked = [b for b in bounds(contract) if actual.get(b.name) == expected[b.name]]
    unchecked = [b.name for b in bounds(contract) if b not in checked]
    rows, nulls, outside = measure_ranges(redshift, contract, month, checked)
    return contract_result(contract, actual, rows, nulls, outside, unchecked)
