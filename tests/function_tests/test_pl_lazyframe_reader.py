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
