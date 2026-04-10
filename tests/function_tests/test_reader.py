"""Regression tests for gluestick.reader.Reader."""

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gluestick.reader import Reader


def _write_parquet(path: Path):
    df = pd.DataFrame(
        {
            "order_id": [1, 2],
            "amount": [5.5, 11.0],
            "created_at": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-02T12:30:00Z"], utc=True
            ),
            "is_active": [True, False],
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def _write_catalog(path: Path, stream_name="orders"):
    catalog = {
        "streams": [
            {
                "stream": stream_name,
                "tap_stream_id": stream_name,
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
    path.write_text(json.dumps(catalog), encoding="utf-8")


def _write_csv(path: Path):
    path.write_text(
        "order_id,amount,created_at,is_active\n"
        "1,5.5,2024-01-01T00:00:00Z,true\n"
        "2,11.0,2024-01-02T12:30:00Z,false\n",
        encoding="utf-8",
    )


def test_get_returns_default_when_stream_missing(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get("nope") is None
    sentinel = pd.DataFrame({"a": [1]})
    assert reader.get("nope", default=sentinel) is sentinel


def test_get_parquet_roundtrip(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders")
    assert len(df) == 2
    assert list(df.columns) == ["order_id", "amount", "created_at", "is_active"]


def test_get_parquet_catalog_types(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    _write_catalog(tmp_path / "catalog.json")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)
    assert df["order_id"].dtype.name == "Int64"
    assert df["amount"].dtype == float
    assert pd.api.types.is_datetime64_any_dtype(df["created_at"])
    assert df["is_active"].dtype.name == "boolean"


def test_get_csv_with_catalog_parse_dates(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)
    _write_catalog(tmp_path / "catalog.json")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)
    assert df["order_id"].dtype.name == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(df["created_at"])


def test_get_csv_chunked_returns_reader_and_parse_dates(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)
    _write_catalog(tmp_path / "catalog.json")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    out = reader.get("orders", catalog_types=True, chunksize=1)
    chunk_reader, parse_dates = out
    assert parse_dates == ["created_at"]
    chunks = list(chunk_reader)
    assert len(chunks) == 2


def test_read_parquet_with_chunks(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    gen = reader.read_parquet_with_chunks(str(pq_path), chunksize=1)
    parts = list(gen)
    assert len(parts) == 2
    assert len(parts[0]) == 1


def test_get_with_chunksize_uses_chunk_reader(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    gen = reader.get("orders", chunksize=1)
    parts = list(gen)
    assert len(parts) == 2


def test_read_directories_strips_version_suffix(tmp_path):
    (tmp_path / "orders-123.parquet").write_bytes(b"")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert "orders" in reader.input_files


def test_read_directories_single_file_path(tmp_path):
    pq_path = tmp_path / "only.parquet"
    _write_parquet(pq_path)
    reader = Reader(dir=str(pq_path), root=str(tmp_path))
    assert reader.input_files.get("only") == str(pq_path)


def test_get_metadata_parquet(tmp_path):
    arr = pa.array([1, 2])
    table = pa.table({"id": arr})
    meta = {b"key_properties": b"['id']"}
    table = table.replace_schema_metadata(meta)
    pq_path = tmp_path / "meta.parquet"
    pq.write_table(table, pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    md = reader.get_metadata("meta")
    assert md.get("key_properties") == "['id']"


def test_get_pk_from_parquet_metadata(tmp_path):
    arr = pa.array([1, 2])
    table = pa.table({"id": arr})
    meta = {b"key_properties": b"['id']"}
    table = table.replace_schema_metadata(meta)
    pq_path = tmp_path / "pk.parquet"
    pq.write_table(table, pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get_pk("pk") == ["id"]


def test_get_pk_from_catalog_csv(tmp_path):
    csv_path = tmp_path / "lineitems.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    catalog = {
        "streams": [
            {
                "stream": "lineitems",
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {"table-key-properties": ["a", "b"]},
                    }
                ],
            }
        ]
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get_pk("lineitems") == ["a", "b"]


def test_get_types_from_catalog_with_headers(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    _write_catalog(tmp_path / "catalog.json")
    catalog = json.loads((tmp_path / "catalog.json").read_text(encoding="utf-8"))
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    headers = ["order_id", "amount", "created_at", "is_active"]
    types = reader.get_types_from_catalog(catalog, "orders", headers=headers)
    assert types["parse_dates"] == ["created_at"]
    assert types["dtype"]["order_id"] == "Int64"
    assert types["dtype"]["amount"] is float
    assert types["dtype"]["is_active"] == "boolean"


def test_clean_catalog_and_read_target_catalog(tmp_path):
    target = {
        "streams": [
            {
                "stream": "orders",
                "schema": {
                    "properties": {"id": {"type": ["integer", "null"]}},
                },
            }
        ]
    }
    path = tmp_path / "target-catalog.json"
    path.write_text(json.dumps(target), encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    raw = reader.read_target_catalog(process_schema=False)
    assert raw == target
    raw2, clean = reader.read_target_catalog(process_schema=True)
    assert raw2 == target
    assert clean == {"orders": {"id": {"type": ["integer", "null"]}}}


def test_read_catalog_missing_returns_none(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.read_catalog() is None


def test_get_types_from_catalog_unknown_stream(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    catalog = {
        "streams": [
            {
                "stream": "x",
                "tap_stream_id": "x",
                "schema": {"properties": {}},
            }
        ]
    }
    # headers= avoids pd.read_csv when stream is absent from input_files (filepath is None)
    assert reader.get_types_from_catalog(catalog, "missing", headers=[]) == {}


def test_reader_str_repr_list_stream_keys(tmp_path):
    (tmp_path / "items.csv").write_text("k\n1\n", encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert list(reader.input_files.keys()) == ["items"]
    assert "items" in str(reader)
    assert "items" in repr(reader)


def test_get_metadata_missing_stream_raises(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="no file for stream"):
        reader.get_metadata("missing")


def test_get_metadata_non_parquet_returns_empty_dict(tmp_path):
    (tmp_path / "plain.csv").write_text("a\n1\n", encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get_metadata("plain") == {}


def test_get_pk_parquet_without_key_properties(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get_pk("orders") == []


def test_get_pk_csv_no_catalog(tmp_path):
    (tmp_path / "solo.csv").write_text("a\n1\n", encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.get_pk("solo") == []


def test_read_directories_respects_ignore(tmp_path):
    (tmp_path / "keep.csv").write_text("a\n1\n", encoding="utf-8")
    (tmp_path / "skip.csv").write_text("b\n2\n", encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    out = reader.read_directories(ignore=["skip"])
    assert "skip" not in out
    assert "keep" in out


def test_read_directories_skips_non_tabular_files(tmp_path):
    (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
    (tmp_path / "data.csv").write_text("a\n1\n", encoding="utf-8")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert "data" in reader.input_files
    assert "notes" not in reader.input_files


def test_read_directories_duplicate_entity_keeps_single_key(tmp_path):
    (tmp_path / "dup-1.parquet").write_bytes(b"")
    (tmp_path / "dup-2.parquet").write_bytes(b"")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert list(reader.input_files.keys()) == ["dup"]


def test_get_csv_catalog_types_false_raw_read(tmp_path):
    _write_csv(tmp_path / "orders.csv")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=False)
    assert len(df) == 2


def test_get_parquet_catalog_types_false(tmp_path):
    pq_path = tmp_path / "orders.parquet"
    _write_parquet(pq_path)
    _write_catalog(tmp_path / "catalog.json")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=False)
    assert len(df) == 2


def test_get_types_from_catalog_anyof_prefers_date_time(tmp_path):
    catalog = {
        "streams": [
            {
                "stream": "s",
                "tap_stream_id": "s",
                "schema": {
                    "properties": {
                        "ts": {
                            "anyOf": [
                                {"type": "null"},
                                {"type": "string", "format": "date-time"},
                            ]
                        }
                    }
                },
            }
        ]
    }
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    r = reader.get_types_from_catalog(catalog, "s", headers=["ts"])
    assert r["parse_dates"] == ["ts"]
    assert "ts" not in r.get("dtype", {})


def test_get_types_from_catalog_matches_tap_stream_id(tmp_path):
    catalog = {
        "streams": [
            {
                "stream": "internal_name",
                "tap_stream_id": "orders",
                "schema": {
                    "properties": {"order_id": {"type": ["integer", "null"]}}
                },
            }
        ]
    }
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    r = reader.get_types_from_catalog(catalog, "orders", headers=["order_id"])
    assert r["dtype"]["order_id"] == "Int64"


def test_get_types_from_catalog_reads_headers_from_csv_file(tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_csv(csv_path)
    _write_catalog(tmp_path / "catalog.json")
    catalog = json.loads((tmp_path / "catalog.json").read_text(encoding="utf-8"))
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    r = reader.get_types_from_catalog(catalog, "orders")
    assert "order_id" in r["dtype"]
    assert "created_at" in r["parse_dates"]


def test_get_types_from_catalog_multi_type_column_defaults_to_object(tmp_path):
    catalog = {
        "streams": [
            {
                "stream": "s",
                "tap_stream_id": "s",
                "schema": {
                    "properties": {
                        "mix": {"type": ["string", "integer", "null"]},
                    }
                },
            }
        ]
    }
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    r = reader.get_types_from_catalog(catalog, "s", headers=["mix"])
    assert r["dtype"]["mix"] == "object"


def test_clean_catalog_uses_tap_stream_id_when_stream_missing(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    catalog = {
        "streams": [
            {
                "tap_stream_id": "from_tap",
                "schema": {"properties": {"x": {"type": "string"}}},
            }
        ]
    }
    clean = reader.clean_catalog(catalog)
    assert "from_tap" in clean
    assert "x" in clean["from_tap"]


def test_read_target_catalog_missing_returns_none(tmp_path):
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    assert reader.read_target_catalog() is None


def test_get_csv_null_date_cell_coerces_to_nat(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text(
        "order_id,amount,created_at,is_active\n"
        "1,5.5,,true\n",
        encoding="utf-8",
    )
    _write_catalog(tmp_path / "catalog.json")
    reader = Reader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.get("orders", catalog_types=True)
    assert pd.isna(df["created_at"].iloc[0])
