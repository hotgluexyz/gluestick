import json
from pathlib import Path

import pytest
import polars as pl

from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader
from gluestick.utils.polars_utils import map_pd_type_to_polars, cast_lf_from_schema


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


def _write_catalog(path: Path):
    catalog = {
        "streams": [
            {
                "stream": "orders",
                "schema": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": ["integer", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "created_at": {"type": "string", "format": "date-time"},
                        "is_active": {"type": ["boolean", "null"]},
                    },
                },
            }
        ]
    }
    path.write_text(json.dumps(catalog))


def test_get_parquet_applies_catalog_types(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get_parquet("orders", str(parquet_path), catalog_types=True)
    df = lf.collect()

    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["created_at"] == pl.Utf8
    assert df.schema["is_active"] == pl.Boolean
    assert df["order_id"][0] == 1
    assert df["amount"][0] == 5.5
    assert df["is_active"][0] is True


def test_get_parquet_without_catalog_does_not_cast(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get_parquet("orders", str(parquet_path), catalog_types=True)
    df = lf.collect()

    assert df.schema["order_id"] == pl.Utf8
    assert df.schema["amount"] == pl.Utf8
    assert df.schema["created_at"] == pl.Utf8
    assert df.schema["is_active"] == pl.Utf8


def test_get_types_from_catalog_maps_datetime_and_numbers():
    catalog = {
        "streams": [
            {
                "stream": "orders",
                "schema": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": ["integer", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "created_at": {"type": "string", "format": "date-time"},
                        "is_active": {"type": ["boolean", "null"]},
                    },
                },
            }
        ]
    }
    headers = ["order_id", "amount", "created_at", "is_active"]
    reader = PLLazyFrameReader(dir=".", root=".")

    types = reader.get_types_from_catalog(catalog, "orders", headers=headers)

    assert types["order_id"] == pl.Int64
    assert types["amount"] == pl.Float64
    assert "created_at" not in types
    assert types["is_active"] == pl.Boolean


def test_map_pd_type_to_polars_mappings():
    assert map_pd_type_to_polars("Int64") == pl.Int64
    assert map_pd_type_to_polars("Float64") == pl.Float64
    assert map_pd_type_to_polars("float") == pl.Float64
    assert map_pd_type_to_polars("boolean") == pl.Boolean
    assert map_pd_type_to_polars("object") == pl.String
    assert map_pd_type_to_polars("Datetime") == pl.Datetime("ns", "UTC")

    with pytest.raises(ValueError, match="Unknown type"):
        map_pd_type_to_polars("NotAType")


def test_cast_lf_from_schema_casts_columns():
    lf = pl.LazyFrame({"order_id": ["1"], "amount": ["5.5"]})
    types = {"order_id": pl.Int64, "amount": pl.Float64}

    casted = cast_lf_from_schema(lf, types).collect()

    assert casted.schema["order_id"] == pl.Int64
    assert casted.schema["amount"] == pl.Float64
    assert casted["order_id"][0] == 1
    assert casted["amount"][0] == 5.5


def _write_csv(path: Path):
    path.write_text(
        "order_id,amount,created_at,is_active\n"
        "1,5.5,2024-01-01T00:00:00Z,true\n"
        "2,11.0,2024-01-02T12:30:00Z,false\n"
    )


def _write_csv_no_datetime(path: Path):
    """CSV without date-time column - PLLazyFrameReader scan_csv schema omits parse_dates fields."""
    path.write_text(
        "order_id,amount,is_active\n"
        "1,5.5,true\n"
        "2,11.0,false\n"
    )


def _write_catalog_no_datetime(path: Path):
    catalog = {
        "streams": [
            {
                "stream": "orders",
                "schema": {
                    "type": "object",
                    "properties": {
                        "order_id": {"type": ["integer", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "is_active": {"type": ["boolean", "null"]},
                    },
                },
            }
        ]
    }
    path.write_text(json.dumps(catalog))


# ---- get() parity with PolarsReader ----
def test_get_returns_default_when_stream_missing(tmp_path):
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get("nope") is None
    sentinel = pl.LazyFrame({"a": [1]})
    assert reader.get("nope", default=sentinel) is sentinel


def test_get_parquet_catalog_types_false_no_cast(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)
    _write_catalog(tmp_path / "catalog.json")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get("orders", catalog_types=False)
    df = lf.collect()

    assert df.schema["order_id"] == pl.Utf8


def test_get_csv_applies_catalog_types(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv_no_datetime(csv_path)
    _write_catalog_no_datetime(tmp_path / "catalog.json")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get("orders", catalog_types=True)
    df = lf.collect()

    assert df.schema["order_id"] == pl.Int64
    assert df.schema["amount"] == pl.Float64
    assert df.schema["is_active"] == pl.Boolean


def test_get_csv_without_catalog_raw_scan(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get("orders", catalog_types=False)
    df = lf.collect()

    assert df.height == 2
    assert "order_id" in df.columns


def test_get_raises_value_error_for_unsupported_file_type(tmp_path):
    bad = tmp_path / "x.dat"
    bad.write_bytes(b"x")
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    reader.input_files["bad"] = str(bad)

    with pytest.raises(ValueError, match="Unsupported file type"):
        reader.get("bad")


# ---- get_csv / get_parquet direct ----
def test_get_csv_method_catalog_types_false(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get_csv("orders", str(csv_path), catalog_types=False)
    assert lf.collect().height == 2


def test_get_parquet_catalog_types_false(tmp_path):
    parquet_path = tmp_path / "orders.parquet"
    _write_parquet(parquet_path)

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.get_parquet("orders", str(parquet_path), catalog_types=False)
    assert lf.collect().schema["order_id"] == pl.Utf8


def test_get_types_from_catalog_missing_stream_returns_empty():
    catalog = {"streams": []}
    reader = PLLazyFrameReader(dir=".", root=".")
    assert reader.get_types_from_catalog(catalog, "x", headers=["a"]) == {}


# ---- read_snapshots ----
def test_read_snapshots_parquet(tmp_path):
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    pl.DataFrame({"id": [1]}).write_parquet(snap_dir / "orders.snapshot.parquet")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.read_snapshots("orders", str(snap_dir))
    assert lf is not None
    assert lf.collect().shape == (1, 1)


def test_read_snapshots_csv(tmp_path):
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    (snap_dir / "orders.snapshot.csv").write_text("id\n1\n")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.read_snapshots("orders", str(snap_dir))
    assert lf is not None
    assert lf.collect().height == 1


def test_read_snapshots_neither_exists(tmp_path):
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.read_snapshots("orders", str(snap_dir)) is None


# ---- snapshot_records (first write only) ----
def test_snapshot_records_first_write_parquet(tmp_path):
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    lf_in = pl.LazyFrame({"id": [1], "name": ["a"]})
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))

    out = reader.snapshot_records(lf_in, "orders", str(snap_dir))
    assert out is not None
    assert (snap_dir / "orders.snapshot.parquet").exists()
    loaded = pl.read_parquet(snap_dir / "orders.snapshot.parquet")
    assert loaded.height == 1
