import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import to_singer_schema


class TestToSingerSchema:
    def test_string(self):
        assert to_singer_schema("hello") == {"type": ["string", "null"]}

    def test_integer(self):
        assert to_singer_schema(42) == {"type": ["integer", "null"]}

    def test_float(self):
        assert to_singer_schema(19.99) == {"type": ["number", "null"]}

    def test_boolean_not_treated_as_int(self):
        assert to_singer_schema(True) == {"type": ["boolean", "null"]}

    def test_unknown_types_fallback_to_string(self):
        assert to_singer_schema(None) == {"type": ["string", "null"]}
        assert to_singer_schema((1, 2)) == {"type": ["string", "null"]}

    def test_empty_dict(self):
        assert to_singer_schema({}) == {"type": ["object", "null"], "properties": {}}

    def test_dict_recurses_on_values(self):
        result = to_singer_schema({"customer_name": "alice", "order_total": 59.99})
        assert result["type"] == ["object", "null"]
        assert result["properties"]["customer_name"] == {"type": ["string", "null"]}
        assert result["properties"]["order_total"] == {"type": ["number", "null"]}

    def test_empty_list(self):
        assert to_singer_schema([]) == {
            "items": {"type": ["string", "null"]},
            "type": ["array", "null"],
        }

    def test_non_empty_list_uses_first_element(self):
        result = to_singer_schema([100, "ignored", 3.14])
        assert result == {"type": ["array", "null"], "items": {"type": ["integer", "null"]}}

    def test_complex_nested_structure(self):
        result = to_singer_schema({
            "tenant_id": "org-123",
            "employee_count": 50,
            "annual_revenue": 1_500_000.00,
            "is_active": True,
            "departments": ["engineering", "sales"],
            "headquarters": {"city": "Austin", "state": "TX"},
            "past_orders": [],
        })
        props = result["properties"]
        assert props["tenant_id"] == {"type": ["string", "null"]}
        assert props["employee_count"] == {"type": ["integer", "null"]}
        assert props["annual_revenue"] == {"type": ["number", "null"]}
        assert props["is_active"] == {"type": ["boolean", "null"]}
        assert props["departments"] == {"type": ["array", "null"], "items": {"type": ["string", "null"]}}
        assert props["headquarters"]["type"] == ["object", "null"]
        assert props["headquarters"]["properties"]["city"] == {"type": ["string", "null"]}
        assert props["past_orders"] == {"items": {"type": ["string", "null"]}, "type": ["array", "null"]}
