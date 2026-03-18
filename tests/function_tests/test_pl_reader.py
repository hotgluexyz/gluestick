import json
from pathlib import Path

import pytest
import polars as pl

from gluestick.readers.pl_reader import PolarsReader


def _write_parquet(path: Path):
    df = pl.DataFrame(
        {
            "order_id": ["1", "2"],
            "amount": ["5.5", "11.0"],
            "created_at": ["2024-01-01T00:00:00Z", "2024-01-02T12:30:00Z"],
            "is_active": ["true", "false"],
        }
    )
    df.write_parquet(path)


def _write_catalog(path: Path, stream_name="orders"):
    catalog = {
        "streams": [
            {
                "stream": stream_name,
                "schema": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": ["integer", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "created_at": {
                            "type": "string",
                            "format": "date-time",
                        },
                        "is_active": {"type": ["boolean", "null"]},
                    },
                },
            }
        ]
    }
    path.write_text(json.dumps(catalog))


def _write_csv(path: Path):
    path.write_text(
        "order_id,amount,created_at,is_active\n"
        "1,5.5,2024-01-01T00:00:00Z,true\n"
        "2,11.0,2024-01-02T12:30:00Z,false\n"
    )


# ---- get() ----
def test_get_returns_default_when_stream_not_in_input_files(tmp_path):
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get("nonexistent") is None
    sentinel = pl.DataFrame({"a": [1]})
    got = reader.get("nonexistent", default=sentinel)
    assert got is sentinel

def test_get_parquet_returns_dataframe_with_catalog_types(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)

    assert isinstance(df, pl.DataFrame)
    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["created_at"] == pl.Utf8
    assert df.schema["is_active"] == pl.Boolean
    assert df["order_id"][0] == 1
    assert df["amount"][0] == 5.5
    assert df["is_active"][0] is True

def test_get_parquet_without_catalog_keeps_string_types(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)

    assert df.schema["order_id"] == pl.Utf8
    assert df.schema["amount"] == pl.Utf8
    assert df.schema["is_active"] == pl.Utf8

def test_get_csv_returns_dataframe_with_catalog_types(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)

    assert isinstance(df, pl.DataFrame)
    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["is_active"] == pl.Boolean
    assert df["order_id"][0] == 1
    assert df["amount"][0] == 5.5
    assert df["is_active"][0] is True

def test_get_csv_without_catalog_raw_read_via_get(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=False)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 2
    assert "order_id" in df.columns and "amount" in df.columns

def test_get_raises_value_error_for_unsupported_file_type(tmp_path):
    xyz_path = tmp_path / "foo.xyz"
    xyz_path.write_bytes(b"dummy")
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    reader.input_files["foo"] = str(xyz_path)

    with pytest.raises(ValueError, match="Unsupported file type"):
        reader.get("foo")

# ---- get_parquet() ----
def test_get_parquet_applies_catalog_types(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get_parquet("orders", str(parquet_path), catalog_types=True)

    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["is_active"] == pl.Boolean
    assert df["order_id"][0] == 1
    assert df["amount"][0] == 5.5
    assert df["is_active"][0] is True

def test_get_parquet_without_catalog_does_not_cast(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get_parquet("orders", str(parquet_path), catalog_types=True)

    assert df.schema["order_id"] == pl.Utf8
    assert df.schema["amount"] == pl.Utf8

def test_get_parquet_catalog_types_false_no_cast(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get_parquet("orders", str(parquet_path), catalog_types=False)

    assert df.schema["order_id"] == pl.Utf8
    assert df.schema["amount"] == pl.Utf8

# ---- get_csv() ----
def test_get_csv_applies_catalog_types(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get_csv("orders", str(csv_path), catalog_types=True)

    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["is_active"] == pl.Boolean
    assert df["order_id"][0] == 1
    assert df["amount"][0] == 5.5

def test_get_csv_method_without_catalog_raw_read(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get_csv("orders", str(csv_path), catalog_types=False)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 2
    assert "order_id" in df.columns and "amount" in df.columns

# ---- get_types_from_catalog() ----
def test_get_types_from_catalog_maps_types():
    catalog = {
        "streams": [
            {
                "stream": "orders",
                "schema": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": ["integer", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "created_at": {
                            "type": "string",
                            "format": "date-time",
                        },
                        "is_active": {"type": ["boolean", "null"]},
                    },
                },
            }
        ]
    }
    headers = ["order_id", "amount", "created_at", "is_active"]
    reader = PolarsReader(dir=".", root=".")

    types = reader.get_types_from_catalog(catalog, "orders", headers=headers)

    assert types["order_id"] == pl.Int64
    assert types["amount"] == pl.Float64
    assert "created_at" not in types
    assert types["is_active"] == pl.Boolean

def test_get_types_from_catalog_missing_stream_returns_empty():
    catalog = {"streams": []}
    reader = PolarsReader(dir=".", root=".")
    types = reader.get_types_from_catalog(
        catalog, "missing", headers=["a", "b"]
    )
    assert types == {}

# ---- read_snapshots() ----
def test_read_snapshots_parquet_exists(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    path = snapshot_dir / "orders.snapshot.parquet"
    pl.DataFrame({"id": [1], "name": ["x"]}).write_parquet(path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.read_snapshots("orders", str(snapshot_dir))

    assert df is not None
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 2)
    assert "id" in df.columns
    assert "name" in df.columns

def test_read_snapshots_csv_exists(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    path = snapshot_dir / "orders.snapshot.csv"
    path.write_text("id,name\n1,x\n")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.read_snapshots("orders", str(snapshot_dir))

    assert df is not None
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 2)

def test_read_snapshots_neither_exists(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.read_snapshots("orders", str(snapshot_dir))
    assert df is None

# ---- snapshot_records() ----
def test_snapshot_records_first_write_returns_stream_data(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    stream_data = pl.DataFrame({"id": [1], "name": ["a"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(stream_data, "orders", str(snapshot_dir))

    assert result is stream_data
    assert (snapshot_dir / "orders.snapshot.parquet").exists()
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.equals(stream_data)

def test_snapshot_records_merge_just_new_false_returns_merged(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    existing = pl.DataFrame({"id": [1], "name": ["old"]})
    existing.write_parquet(snapshot_dir / "orders.snapshot.parquet")
    stream_data = pl.DataFrame({"id": [2], "name": ["new"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(
        stream_data, "orders", str(snapshot_dir), pk="id", just_new=False
    )

    assert result is not None
    assert result.shape[0] == 2
    assert set(result["id"].to_list()) == {1, 2}
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.shape[0] == 2

def test_snapshot_records_merge_just_new_true_returns_stream_data(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    snap_path = snapshot_dir / "orders.snapshot.parquet"
    pl.DataFrame({"id": [1], "name": ["old"]}).write_parquet(snap_path)
    stream_data = pl.DataFrame({"id": [2], "name": ["new"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(
        stream_data, "orders", str(snapshot_dir), pk="id", just_new=True
    )

    assert result is stream_data
    assert result.shape[0] == 1
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.shape[0] == 2

def test_snapshot_records_overwrite_replaces_snapshot(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    pl.DataFrame({"id": [1], "name": ["old"]}).write_parquet(
        snapshot_dir / "orders.snapshot.parquet"
    )
    stream_data = pl.DataFrame({"id": [2], "name": ["new"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(
        stream_data, "orders", str(snapshot_dir), overwrite=True
    )

    assert result is stream_data
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.shape[0] == 1
    assert loaded["name"][0] == "new"

def test_snapshot_records_only_existing_snapshot_returns_snapshot(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    existing = pl.DataFrame({"id": [1], "name": ["x"]})
    existing.write_parquet(snapshot_dir / "orders.snapshot.parquet")
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(None, "orders", str(snapshot_dir))

    assert result is not None
    assert result.equals(existing)

def test_snapshot_records_both_none_returns_none(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    result = reader.snapshot_records(None, "orders", str(snapshot_dir))
    assert result is None

def test_snapshot_records_pk_as_list(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    pl.DataFrame({"a": [1], "b": [10], "val": ["old"]}).write_parquet(
        snapshot_dir / "orders.snapshot.parquet"
    )
    stream_data = pl.DataFrame({"a": [2], "b": [20], "val": ["new"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(
        stream_data, "orders", str(snapshot_dir), pk=["a", "b"], just_new=False
    )

    assert result is not None
    assert result.shape[0] == 2
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.shape[0] == 2

def test_snapshot_records_use_csv(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    stream_data = pl.DataFrame({"id": [1], "name": ["a"]})
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(
        stream_data, "orders", str(snapshot_dir), use_csv=True
    )

    assert result is stream_data
    assert (snapshot_dir / "orders.snapshot.csv").exists()
    loaded = pl.read_csv(snapshot_dir / "orders.snapshot.csv")
    assert loaded.shape == (1, 2)
    assert loaded["name"][0] == "a"

# ---- Discovery / integration ----
def test_get_orders_end_to_end_discovery_and_catalog(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders")

    assert df is not None
    assert "orders" in reader.input_files
    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df["order_id"][0] == 1
