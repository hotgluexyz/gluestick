"""Unit tests for the inlined Singer protocol write helpers."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import write_record, write_schema, write_state


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

    def test_output_is_single_line(self, capsys):
        write_schema("s", {}, [])
        out = capsys.readouterr().out
        assert out.count("\n") == 1


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
