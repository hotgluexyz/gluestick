import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from gluestick.singer import remove_nulls_deep


class TestRemoveNullsDeep:
    def test_removes_none_from_dict(self):
        result = remove_nulls_deep({"customer_name": "alice", "deleted_at": None})
        assert result == {"customer_name": "alice"}

    def test_removes_nan_from_dict(self):
        result = remove_nulls_deep({"score": float("nan"), "status": "active"})
        assert result == {"status": "active"}

    def test_removes_none_from_list(self):
        result = remove_nulls_deep(["alice", None, "bob"])
        assert result == ["alice", "bob"]

    def test_removes_nan_from_list(self):
        result = remove_nulls_deep([1.0, float("nan"), 3.0])
        assert result == [1.0, 3.0]

    def test_converts_series_to_dict_first(self):
        series = pd.Series({"order_id": 1, "discount": None, "total": 99.5})
        result = remove_nulls_deep(series)
        assert isinstance(result, dict)
        assert "discount" not in result
        assert result["order_id"] == 1

    def test_recurses_into_nested_dicts(self):
        result = remove_nulls_deep({
            "order": {
                "customer_name": "alice",
                "coupon_code": None,
                "shipping": {"tracking_id": None, "carrier": "ups"},
            }
        })
        assert result == {
            "order": {
                "customer_name": "alice",
                "shipping": {"carrier": "ups"},
            }
        }

    def test_recurses_into_nested_lists(self):
        result = remove_nulls_deep({
            "tags": ["electronics", None, "sale"],
            "items": [{"sku": "A1", "note": None}],
        })
        assert result == {
            "tags": ["electronics", "sale"],
            "items": [{"sku": "A1"}],
        }

    def test_scalar_passthrough(self):
        assert remove_nulls_deep("hello") == "hello"
        assert remove_nulls_deep(42) == 42
        assert remove_nulls_deep(True) is True
