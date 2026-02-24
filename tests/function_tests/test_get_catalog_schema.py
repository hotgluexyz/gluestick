import pytest
import sys
from pathlib import Path
import importlib

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
singer_utils = importlib.import_module("gluestick.singer")


class _FakeReader:
    def read_catalog(self):
        return {
            "streams": [
                {
                    "stream": "orders",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "event_time": {
                                "anyOf": [
                                    {"type": "string"},
                                    {"type": "string", "format": "date-time"},
                                ]
                            },
                            "amount": {
                                "anyOf": [
                                    {"type": "null"},
                                    {"type": ["integer", "null"]},
                                    {"type": "number"},
                                ]
                            },
                            "tags": {"type": "array"},
                            "items_or_null": {"type": ["array", "null"]},
                            "status": {"type": "string"},
                        },
                        "additionalProperties": False,
                    },
                }
            ]
        }


def test_get_catalog_schema_raises_when_stream_not_found(monkeypatch):
    monkeypatch.setattr(singer_utils, "Reader", _FakeReader)

    with pytest.raises(Exception, match="No schema found in catalog for stream missing"):
        singer_utils.get_catalog_schema("missing")


def test_get_catalog_schema_normalizes_anyof_and_arrays(monkeypatch):
    monkeypatch.setattr(singer_utils, "Reader", _FakeReader)

    result = singer_utils.get_catalog_schema("orders")

    assert set(result.keys()) == {"type", "properties"}
    assert result["type"] == "object"

    event_time = result["properties"]["event_time"]
    assert event_time["type"] == "string"
    assert event_time["format"] == "date-time"
    assert "anyOf" not in event_time

    amount = result["properties"]["amount"]
    assert amount["type"] == ["integer", "null", "number"]
    assert "anyOf" not in amount

    assert result["properties"]["tags"]["items"] == {}
    assert result["properties"]["items_or_null"]["items"] == {}
