"""Tests for pandas snapshot helpers in `gluestick.etl_utils` (excluding `drop_redundant`)."""

import pandas as pd
import pytest

from gluestick.etl_utils import (
    get_row_hash,
    merge_id_from_snapshot,
    read_snapshots,
    snapshot_records,
)


def test_read_snapshots_parquet_roundtrip(tmp_path):
    snap = tmp_path / "orders.snapshot.parquet"
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df.to_parquet(snap, index=False)

    out = read_snapshots("orders", str(tmp_path))
    assert out is not None
    assert len(out) == 2
    assert list(out["id"]) == [1, 2]


def test_read_snapshots_csv_roundtrip(tmp_path):
    snap = tmp_path / "orders.snapshot.csv"
    df = pd.DataFrame({"id": [1], "name": ["x"]})
    df.to_csv(snap, index=False)

    out = read_snapshots("orders", str(tmp_path))
    assert out is not None
    assert len(out) == 1


def test_read_snapshots_csv_passes_kwargs_to_read_csv(tmp_path):
    snap = tmp_path / "orders.snapshot.csv"
    pd.DataFrame({"id": [1]}).to_csv(snap, index=False)

    out = read_snapshots("orders", str(tmp_path), dtype={"id": "Int64"})
    assert out["id"].dtype.name == "Int64"


def test_read_snapshots_missing_returns_none(tmp_path):
    assert read_snapshots("nope", str(tmp_path)) is None


def test_read_snapshots_parquet_preferred_when_both_exist(tmp_path):
    """Parquet branch is checked first in `read_snapshots`."""
    pd.DataFrame({"id": [1]}).to_parquet(tmp_path / "orders.snapshot.parquet", index=False)
    pd.DataFrame({"id": [99]}).to_csv(tmp_path / "orders.snapshot.csv", index=False)

    out = read_snapshots("orders", str(tmp_path))
    assert int(out["id"].iloc[0]) == 1


def test_snapshot_records_first_write_writes_parquet(tmp_path):
    incoming = pd.DataFrame({"id": [1, 2], "v": [10, 20]})
    result = snapshot_records(incoming, "orders", str(tmp_path), pk="id")

    assert result is incoming
    assert (tmp_path / "orders.snapshot.parquet").is_file()
    loaded = pd.read_parquet(tmp_path / "orders.snapshot.parquet")
    assert len(loaded) == 2


def test_snapshot_records_merge_dedupes_on_pk_keep_last(tmp_path):
    """After concat, `drop_duplicates(pk, keep='last')` - last row wins per key."""
    existing = pd.DataFrame({"id": [1], "v": ["old"]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    delta = pd.DataFrame({"id": [1, 2], "v": ["new", "b"]})
    merged = snapshot_records(delta, "orders", str(tmp_path), pk="id", just_new=False)

    assert len(merged) == 2
    row = merged[merged["id"] == 1].iloc[0]
    assert row["v"] == "new"

    loaded = pd.read_parquet(tmp_path / "orders.snapshot.parquet")
    assert len(loaded) == 2
    assert loaded[loaded["id"] == 1]["v"].iloc[0] == "new"


def test_snapshot_records_merge_composite_pk_drop_duplicates(tmp_path):
    existing = pd.DataFrame({"a": [1], "b": [1], "v": ["x"]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    delta = pd.DataFrame({"a": [1, 2], "b": [1, 2], "v": ["y", "z"]})
    merged = snapshot_records(
        delta, "orders", str(tmp_path), pk=["a", "b"], just_new=False
    )

    assert len(merged) == 2
    assert merged[(merged["a"] == 1) & (merged["b"] == 1)]["v"].iloc[0] == "y"


def test_snapshot_records_just_new_returns_only_stream_data(tmp_path):
    existing = pd.DataFrame({"id": [1], "v": ["keep"]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    delta = pd.DataFrame({"id": [2], "v": ["only"]})
    result = snapshot_records(delta, "orders", str(tmp_path), pk="id", just_new=True)

    assert len(result) == 1
    assert result["id"].iloc[0] == 2

    loaded = pd.read_parquet(tmp_path / "orders.snapshot.parquet")
    assert len(loaded) == 2


def test_snapshot_records_overwrite_replaces_without_merge(tmp_path):
    existing = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    fresh = pd.DataFrame({"id": [9], "v": ["solo"]})
    result = snapshot_records(
        fresh, "orders", str(tmp_path), pk="id", overwrite=True
    )

    assert result is fresh
    loaded = pd.read_parquet(tmp_path / "orders.snapshot.parquet")
    assert len(loaded) == 1
    assert loaded["id"].iloc[0] == 9


def test_snapshot_records_stream_none_returns_existing_snapshot(tmp_path):
    existing = pd.DataFrame({"id": [5], "v": ["p"]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    result = snapshot_records(None, "orders", str(tmp_path), pk="id")
    assert result is not None
    assert len(result) == 1
    assert result["id"].iloc[0] == 5


def test_snapshot_records_no_snapshot_no_stream_returns_none(tmp_path):
    assert snapshot_records(None, "orders", str(tmp_path), pk="id") is None


def test_snapshot_records_use_csv(tmp_path):
    df = pd.DataFrame({"id": [1], "v": [1]})
    snapshot_records(df, "orders", str(tmp_path), pk="id", use_csv=True)
    assert (tmp_path / "orders.snapshot.csv").is_file()


def test_merge_id_from_snapshot_joins_remote_ids(tmp_path):
    snap = pd.DataFrame(
        {"InputId": ["ext-1", "ext-2"], "RemoteId": [100, 200]}
    )
    snap.to_parquet(tmp_path / "orders_flow1.snapshot.parquet", index=False)

    df = pd.DataFrame({"externalId": ["ext-1", "ext-3"], "x": [1, 2]})
    out = merge_id_from_snapshot(df, str(tmp_path), "orders", "flow1", "remote_pk")

    assert "remote_pk" in out.columns
    assert out.loc[out["externalId"] == "ext-1", "remote_pk"].iloc[0] == 100
    assert pd.isna(out.loc[out["externalId"] == "ext-3", "remote_pk"].iloc[0])


def test_merge_id_from_snapshot_no_snapshot_returns_original(tmp_path):
    df = pd.DataFrame({"externalId": ["a"], "x": [1]})
    out = merge_id_from_snapshot(df, str(tmp_path), "orders", "flow1", "rid")
    assert out.equals(df)


def test_merge_id_from_snapshot_raises_without_external_id(tmp_path):
    df = pd.DataFrame({"id": [1]})
    with pytest.raises(Exception, match="externalId"):
        merge_id_from_snapshot(df, str(tmp_path), "orders", "flow1", "pk")


def test_merge_id_from_snapshot_raises_without_pk(tmp_path):
    df = pd.DataFrame({"externalId": ["a"]})
    with pytest.raises(Exception, match="No PK"):
        merge_id_from_snapshot(df, str(tmp_path), "orders", "flow1", "")


def test_get_row_hash_stable_for_same_values():
    row = pd.Series({"b": 2, "a": 1})
    columns = ["a", "b"]
    h1 = get_row_hash(row, columns)
    h2 = get_row_hash(row, columns)
    assert h1 == h2
    assert len(h1) == 32


def test_snapshot_records_coerce_types_bool_to_boolean(tmp_path):
    existing = pd.DataFrame({"id": [1], "flag": [True]})
    existing.to_parquet(tmp_path / "orders.snapshot.parquet", index=False)

    delta = pd.DataFrame({"id": [2], "flag": [False]})
    merged = snapshot_records(
        delta,
        "orders",
        str(tmp_path),
        pk="id",
        coerce_types=True,
        just_new=False,
    )
    assert str(merged["flag"].dtype) == "Boolean" or merged["flag"].dtype.name == "boolean"
