import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import parse_objs


class TestParseObjs:
    def test_non_string_passthrough(self):
        assert parse_objs(42) == 42
        assert parse_objs(None) is None
        assert parse_objs([1, 2]) == [1, 2]
        assert parse_objs({"name": "Azmat"}) == {"name": "Azmat"}

    def test_parses_stringified_dict_via_literal_eval(self):
        result = parse_objs("{'customer_name': 'Azmat', 'age': 30}")
        assert result == {"customer_name": "Azmat", "age": 30}

    def test_parses_stringified_list_via_literal_eval(self):
        result = parse_objs("[1, 2, 3]")
        assert result == [1, 2, 3]

    def test_parses_json_dict(self):
        result = parse_objs('{"customer_name": "Azmat", "age": 30}')
        assert result == {"customer_name": "Azmat", "age": 30}

    def test_parses_json_array(self):
        result = parse_objs('[{"product_id": 1}, {"product_id": 2}]')
        assert result == [{"product_id": 1}, {"product_id": 2}]

    def test_json_fallback_when_literal_eval_fails(self):
        result = parse_objs('{"is_active": true, "score": null}')
        assert result == {"is_active": True, "score": None}
