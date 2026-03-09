import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import datetime

import pandas as pd
import polars as pl
from gluestick.singer import to_singer, gen_singer_header_from_polars_schema


def _read_singer_lines(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _get_records(lines):
    return [line for line in lines if line.get("type") == "RECORD"]


def _get_schema(lines):
    return next(line for line in lines if line.get("type") == "SCHEMA")


class TestToSingerDispatch:
    def test_unsupported_type_raises(self, tmp_path):
        with pytest.raises(NotImplementedError):
            to_singer("not a dataframe", "stream", str(tmp_path))


class TestToSingerBasic:
    def test_writes_schema_records_and_state(self, tmp_path):
        df = pd.DataFrame({"order_id": [1, 2], "total": [10.5, 20.0]})
        to_singer(df, "orders", str(tmp_path))
        lines = _read_singer_lines(tmp_path / "data.singer")

        schema = _get_schema(lines)
        assert schema["stream"] == "orders"
        assert "order_id" in schema["schema"]["properties"]
        assert "total" in schema["schema"]["properties"]

        records = _get_records(lines)
        assert len(records) == 2
        assert records[0]["record"]["order_id"] == 1

        state = [line for line in lines if line.get("type") == "STATE"]
        assert len(state) == 1

    def test_custom_filename(self, tmp_path):
        df = pd.DataFrame({"product_id": [1]})
        to_singer(df, "products", str(tmp_path), filename="custom.singer")
        assert (tmp_path / "custom.singer").exists()

    def test_appends_to_existing_file(self, tmp_path):
        df1 = pd.DataFrame({"customer_id": [1]})
        df2 = pd.DataFrame({"customer_id": [2]})
        to_singer(df1, "customers", str(tmp_path))
        to_singer(df2, "customers", str(tmp_path))
        lines = _read_singer_lines(tmp_path / "data.singer")
        records = _get_records(lines)
        assert len(records) == 2

    def test_keys_passed_to_schema(self, tmp_path):
        df = pd.DataFrame({"invoice_id": [1], "amount": [99.0]})
        to_singer(df, "invoices", str(tmp_path), keys=["invoice_id"])
        schema = _get_schema(_read_singer_lines(tmp_path / "data.singer"))
        assert schema["key_properties"] == ["invoice_id"]


class TestNullHandling:
    def test_nulls_dropped_by_default(self, tmp_path):
        df = pd.DataFrame({"customer_name": ["alice"], "discount": [None]})
        to_singer(df, "customers", str(tmp_path))
        records = _get_records(_read_singer_lines(tmp_path / "data.singer"))
        assert "discount" not in records[0]["record"]

    def test_keep_null_fields_preserves_nulls(self, tmp_path):
        df = pd.DataFrame({"customer_name": ["alice"], "discount": [None]})
        to_singer(df, "customers", str(tmp_path), keep_null_fields=True)
        records = _get_records(_read_singer_lines(tmp_path / "data.singer"))
        assert "discount" in records[0]["record"]
        assert records[0]["record"]["discount"] is None

    def test_trim_nested_nulls(self, tmp_path):
        df = pd.DataFrame({
            "metadata": [{"region": "US", "coupon": None}],
        })
        to_singer(df, "orders", str(tmp_path), allow_objects=True, trim_nested_nulls=True)
        records = _get_records(_read_singer_lines(tmp_path / "data.singer"))
        meta = records[0]["record"]["metadata"]
        assert "region" in meta
        assert "coupon" not in meta


class TestAllowObjects:
    def test_allow_objects_drops_all_null_columns(self, tmp_path):
        df = pd.DataFrame({"customer_name": ["alice"], "empty_col": [None]})
        to_singer(df, "customers", str(tmp_path), allow_objects=True)
        schema = _get_schema(_read_singer_lines(tmp_path / "data.singer"))
        assert "empty_col" not in schema["schema"]["properties"]

    def test_allow_objects_false_serializes_objects(self, tmp_path):
        df = pd.DataFrame({"info": [{"carrier": "ups"}]})
        to_singer(df, "shipments", str(tmp_path), allow_objects=False)
        records = _get_records(_read_singer_lines(tmp_path / "data.singer"))
        assert isinstance(records[0]["record"]["info"], str)


class TestDoesNotMutateInput:
    def test_original_df_unchanged(self, tmp_path):
        df = pd.DataFrame({
            "created_at": pd.to_datetime(["2024-01-01"]),
            "customer_name": ["alice"],
        })
        original_dtype = df["created_at"].dtype
        to_singer(df, "events", str(tmp_path))
        assert df["created_at"].dtype == original_dtype


class TestGenSingerHeaderFromPolarsSchema:
    def test_primitive_types(self):
        schema = pl.Schema({
            "order_id": pl.Int64,
            "total": pl.Float64,
            "is_active": pl.Boolean,
            "customer_name": pl.Utf8,
        })
        header = gen_singer_header_from_polars_schema(schema)
        props = header["properties"]
        assert props["order_id"] == {"type": ["integer", "null"]}
        assert props["total"] == {"type": ["number", "null"]}
        assert props["is_active"] == {"type": ["boolean", "null"]}
        assert props["customer_name"] == {"type": ["string", "null"]}

    def test_date_and_time_types(self):
        schema = pl.Schema({
            "due_date": pl.Date,
            "event_time": pl.Time,
        })
        header = gen_singer_header_from_polars_schema(schema)
        assert header["properties"]["due_date"] == {"type": ["string", "null"], "format": "date"}
        assert header["properties"]["event_time"] == {"type": ["string", "null"], "format": "time"}

    def test_datetime_with_params(self):
        schema = pl.Schema({"created_at": pl.Datetime("us", "UTC")})
        header = gen_singer_header_from_polars_schema(schema)
        assert header["properties"]["created_at"] == {"type": ["string", "null"], "format": "date-time"}

    def test_struct_mapped_to_object(self):
        schema = pl.Schema({"address": pl.Struct({"city": pl.Utf8})})
        header = gen_singer_header_from_polars_schema(schema)
        assert header["properties"]["address"] == {"type": ["object", "null"]}

    def test_list_mapped_to_array(self):
        schema = pl.Schema({"tags": pl.List(pl.Utf8)})
        header = gen_singer_header_from_polars_schema(schema)
        assert header["properties"]["tags"] == {"type": ["array", "null"], "items": {"type": ["any", "null"]}}

    def test_unknown_dtype_defaults_to_string(self):
        schema = pl.Schema({"raw_data": pl.Binary})
        header = gen_singer_header_from_polars_schema(schema)
        assert header["properties"]["raw_data"] == {"type": ["string", "null"]}


class TestPolarsToSinger:
    def test_writes_schema_and_records(self, tmp_path):
        df = pl.DataFrame({"product_id": [1, 2], "price": [9.99, 19.99]})
        to_singer(df, "products", str(tmp_path))
        lines = _read_singer_lines(tmp_path / "data.singer")

        schema = _get_schema(lines)
        assert schema["stream"] == "products"
        assert "product_id" in schema["schema"]["properties"]

        records = _get_records(lines)
        assert len(records) == 2
        assert records[0]["record"]["product_id"] == 1
        assert records[1]["record"]["price"] == 19.99

    def test_datetime_formatted_in_records(self, tmp_path):
        df = pl.DataFrame({"event_at": [datetime.datetime(2024, 3, 15, 10, 30)]})
        to_singer(df, "events", str(tmp_path))
        records = _get_records(_read_singer_lines(tmp_path / "data.singer"))
        assert "T" in records[0]["record"]["event_at"]
        assert records[0]["record"]["event_at"].endswith("Z")

    def test_keys_passed_to_schema(self, tmp_path):
        df = pl.DataFrame({"invoice_id": [1], "amount": [50.0]})
        to_singer(df, "invoices", str(tmp_path), keys=["invoice_id"])
        schema = _get_schema(_read_singer_lines(tmp_path / "data.singer"))
        assert schema["key_properties"] == ["invoice_id"]

    def test_appends_to_existing_file(self, tmp_path):
        df1 = pl.DataFrame({"customer_id": [1]})
        df2 = pl.DataFrame({"customer_id": [2]})
        to_singer(df1, "customers", str(tmp_path))
        to_singer(df2, "customers", str(tmp_path))
        lines = _read_singer_lines(tmp_path / "data.singer")
        records = _get_records(lines)
        assert len(records) == 2

    def test_custom_filename(self, tmp_path):
        df = pl.DataFrame({"sku": ["A100"]})
        to_singer(df, "skus", str(tmp_path), filename="custom.singer")
        assert (tmp_path / "custom.singer").exists()


class TestPolarsLazyFrameToSinger:
    def test_lazyframe_produces_same_output(self, tmp_path):
        lf = pl.LazyFrame({"order_id": [10, 20], "amount": [5.5, 11.0]})
        to_singer(lf, "orders", str(tmp_path))
        lines = _read_singer_lines(tmp_path / "data.singer")

        schema = _get_schema(lines)
        assert schema["stream"] == "orders"
        assert "order_id" in schema["schema"]["properties"]

        records = _get_records(lines)
        assert len(records) == 2
        assert records[0]["record"]["order_id"] == 10
