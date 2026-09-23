"""Unit tests for the inlined Singer protocol write helpers."""

import datetime
import json
import sys
from pathlib import Path

import pytz
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import (
    X_HOTGLUE_KEY,
    build_x_hotglue,
    write_record,
    write_schema,
    write_state,
)


def _capture(capsys, fn, *args, **kwargs):
    fn(*args, **kwargs)
    return [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]


class TestWriteSchema:
    def test_type_field(self, capsys):
        msgs = _capture(capsys, write_schema, "orders", {"properties": {}}, [])
        assert msgs[0]["type"] == "SCHEMA"

    def test_stream_field(self, capsys):
        msgs = _capture(capsys, write_schema, "orders", {"properties": {}}, [])
        assert msgs[0]["stream"] == "orders"

    def test_schema_field_passed_through(self, capsys):
        schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
        msgs = _capture(capsys, write_schema, "items", schema, ["id"])
        assert msgs[0]["schema"] == schema

    def test_key_properties_list(self, capsys):
        msgs = _capture(capsys, write_schema, "invoices", {"properties": {}}, ["invoice_id"])
        assert msgs[0]["key_properties"] == ["invoice_id"]

    def test_key_properties_string_coerced_to_list(self, capsys):
        msgs = _capture(capsys, write_schema, "invoices", {"properties": {}}, "invoice_id")
        assert msgs[0]["key_properties"] == ["invoice_id"]

    def test_key_properties_empty_list(self, capsys):
        msgs = _capture(capsys, write_schema, "events", {"properties": {}}, [])
        assert msgs[0]["key_properties"] == []

    def test_invalid_key_properties_raises(self):
        with pytest.raises(ValueError):
            write_schema("stream", {}, 123)

    def test_bookmark_properties_included_when_set(self, capsys):
        msgs = _capture(capsys, write_schema, "orders", {"properties": {}}, [], bookmark_properties=["updated_at"])
        assert msgs[0]["bookmark_properties"] == ["updated_at"]

    def test_bookmark_properties_omitted_when_not_set(self, capsys):
        msgs = _capture(capsys, write_schema, "orders", {"properties": {}}, [])
        assert "bookmark_properties" not in msgs[0]

    def test_output_is_single_line(self, capsys):
        write_schema("s", {}, [])
        out = capsys.readouterr().out
        assert out.count("\n") == 1

    def test_x_hotglue_included_when_set(self, capsys):
        x_hotglue = {
            "target_state_fields": ["email"],
            "target_state_include_hash": True,
        }
        msgs = _capture(capsys, write_schema, "contacts", {"properties": {}}, [], x_hotglue=x_hotglue)
        assert msgs[0][X_HOTGLUE_KEY] == x_hotglue

    def test_x_hotglue_omitted_when_not_set(self, capsys):
        msgs = _capture(capsys, write_schema, "contacts", {"properties": {}}, [])
        assert X_HOTGLUE_KEY not in msgs[0]


class TestBuildXHotglue:
    def test_returns_none_when_unset(self):
        assert build_x_hotglue() is None
        assert build_x_hotglue(target_state_fields=[]) is None
        assert build_x_hotglue(target_state_include_hash=False) is None

    def test_fields_only(self):
        assert build_x_hotglue(target_state_fields=["email", "status"]) == {
            "target_state_fields": ["email", "status"],
        }

    def test_single_field_string_coerced_to_list(self):
        assert build_x_hotglue(target_state_fields="email") == {
            "target_state_fields": ["email"],
        }

    def test_include_hash_only(self):
        assert build_x_hotglue(target_state_include_hash=True) == {
            "target_state_include_hash": True,
        }

    def test_fields_and_include_hash(self):
        assert build_x_hotglue(
            target_state_fields=["email"],
            target_state_include_hash=True,
        ) == {
            "target_state_fields": ["email"],
            "target_state_include_hash": True,
        }

    def test_invalid_fields_type_raises(self):
        with pytest.raises(ValueError):
            build_x_hotglue(target_state_fields=123)

    def test_non_string_field_names_raise(self):
        with pytest.raises(ValueError):
            build_x_hotglue(target_state_fields=["email", 1])


class TestWriteRecord:
    def test_type_field(self, capsys):
        msgs = _capture(capsys, write_record, "orders", {"id": 1})
        assert msgs[0]["type"] == "RECORD"

    def test_stream_field(self, capsys):
        msgs = _capture(capsys, write_record, "orders", {"id": 1})
        assert msgs[0]["stream"] == "orders"

    def test_record_field(self, capsys):
        record = {"id": 42, "name": "widget", "price": 9.99}
        msgs = _capture(capsys, write_record, "products", record)
        assert msgs[0]["record"] == record

    def test_nested_record(self, capsys):
        record = {"id": 1, "address": {"city": "NYC", "zip": "10001"}}
        msgs = _capture(capsys, write_record, "contacts", record)
        assert msgs[0]["record"]["address"]["city"] == "NYC"

    def test_version_included_when_set(self, capsys):
        msgs = _capture(capsys, write_record, "orders", {"id": 1}, version=2)
        assert msgs[0]["version"] == 2

    def test_version_omitted_when_not_set(self, capsys):
        msgs = _capture(capsys, write_record, "orders", {"id": 1})
        assert "version" not in msgs[0]

    def test_time_extracted_included_and_formatted_as_utc(self, capsys):
        ts = datetime.datetime(2024, 3, 15, 10, 30, 0, tzinfo=pytz.utc)
        msgs = _capture(capsys, write_record, "orders", {"id": 1}, time_extracted=ts)
        assert msgs[0]["time_extracted"] == "2024-03-15T10:30:00.000000Z"

    def test_time_extracted_converted_to_utc(self, capsys):
        eastern = pytz.timezone("US/Eastern")
        ts = eastern.localize(datetime.datetime(2024, 3, 15, 10, 30, 0))
        msgs = _capture(capsys, write_record, "orders", {"id": 1}, time_extracted=ts)
        assert msgs[0]["time_extracted"] == "2024-03-15T14:30:00.000000Z"

    def test_time_extracted_naive_raises(self):
        with pytest.raises(ValueError):
            write_record("orders", {"id": 1}, time_extracted=datetime.datetime(2024, 1, 1))

    def test_time_extracted_omitted_when_not_set(self, capsys):
        msgs = _capture(capsys, write_record, "orders", {"id": 1})
        assert "time_extracted" not in msgs[0]

    def test_output_is_single_line(self, capsys):
        write_record("s", {"k": "v"})
        out = capsys.readouterr().out
        assert out.count("\n") == 1


class TestWriteState:
    def test_type_field(self, capsys):
        msgs = _capture(capsys, write_state, {})
        assert msgs[0]["type"] == "STATE"

    def test_value_field(self, capsys):
        state = {"bookmarks": {"orders": "2024-01-01T00:00:00Z"}}
        msgs = _capture(capsys, write_state, state)
        assert msgs[0]["value"] == state

    def test_empty_state(self, capsys):
        msgs = _capture(capsys, write_state, {})
        assert msgs[0]["value"] == {}

    def test_output_is_single_line(self, capsys):
        write_state({})
        out = capsys.readouterr().out
        assert out.count("\n") == 1
