import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import unwrap_json_schema


class TestUnwrapJsonSchema:
    def test_passthrough_simple_schema(self):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        result = unwrap_json_schema(schema)
        assert result == {"type": "object", "properties": {"name": {"type": "string"}}}

    def test_strips_scalar_title_but_keeps_list_required(self):
        schema = {
            "type": "object",
            "title": "Customer",
            "required": ["customer_name"],
            "properties": {
                "customer_name": {"type": "string", "title": "Name"},
            },
        }
        result = unwrap_json_schema(schema)
        assert "title" not in result
        assert "title" not in result["properties"]["customer_name"]
        assert result["required"] == ["customer_name"]

    def test_resolves_ref(self):
        schema = {
            "type": "object",
            "properties": {
                "billing_address": {"$ref": "#/$defs/Address"},
            },
            "$defs": {
                "Address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                },
            },
        }
        result = unwrap_json_schema(schema)
        assert "$defs" not in result
        addr = result["properties"]["billing_address"]
        assert addr["type"] == "object"
        assert addr["properties"]["city"] == {"type": "string"}

    def test_anyof_null_only_returns_empty_dict(self):
        schema = {
            "type": "object",
            "properties": {
                "deleted_at": {"anyOf": [{"type": "null"}]},
            },
        }
        result = unwrap_json_schema(schema)
        assert result["properties"]["deleted_at"] == {}

    def test_anyof_merges_types(self):
        schema = {
            "type": "object",
            "properties": {
                "amount": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "integer"},
                        {"type": "null"},
                    ]
                },
            },
        }
        result = unwrap_json_schema(schema)
        assert result["properties"]["amount"]["type"] == ["string", "integer", "null"]

    def test_anyof_with_nested_properties(self):
        schema = {
            "type": "object",
            "properties": {
                "shipping_info": {
                    "anyOf": [
                        {"type": "object", "properties": {"carrier": {"type": "string"}}},
                        {"type": "null"},
                    ]
                },
            },
        }
        result = unwrap_json_schema(schema)
        info = result["properties"]["shipping_info"]
        assert info["type"] == ["object", "null"]
        assert "carrier" in info["properties"]

    def test_nested_ref_inside_anyof(self):
        schema = {
            "type": "object",
            "properties": {
                "contact": {
                    "anyOf": [
                        {"$ref": "#/$defs/Contact"},
                        {"type": "null"},
                    ]
                },
            },
            "$defs": {
                "Contact": {
                    "type": "object",
                    "properties": {"email": {"type": "string"}},
                },
            },
        }
        result = unwrap_json_schema(schema)
        assert "$defs" not in result
        contact = result["properties"]["contact"]
        assert "null" in contact["type"]

    def test_defs_removed_from_output(self):
        schema = {
            "type": "object",
            "$defs": {"Unused": {"type": "string"}},
            "properties": {},
        }
        result = unwrap_json_schema(schema)
        assert "$defs" not in result

    def test_list_values_recursed(self):
        schema = {
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": [{"type": "string"}, {"type": "integer"}],
                },
            },
        }
        result = unwrap_json_schema(schema)
        items = result["properties"]["tags"]["items"]
        assert isinstance(items, list)
        assert items[0] == {"type": "string"}
        assert items[1] == {"type": "integer"}
