import sys
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gluestick.singer import deep_convert_datetimes


class TestDeepConvertDatetimes:
    def test_datetime_formatted(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 0)
        assert deep_convert_datetimes(dt) == "2024-03-15T10:30:00.000000Z"

    def test_date_formatted(self):
        d = datetime.date(2024, 3, 15)
        assert deep_convert_datetimes(d) == "2024-03-15"

    def test_non_date_passthrough(self):
        assert deep_convert_datetimes("hello") == "hello"
        assert deep_convert_datetimes(42) == 42
        assert deep_convert_datetimes(None) is None

    def test_list_recurses(self):
        result = deep_convert_datetimes([
            datetime.datetime(2024, 1, 1, 0, 0),
            "NOT A DATE",
            datetime.date(2024, 6, 15),
        ])
        assert result == ["2024-01-01T00:00:00.000000Z", "NOT A DATE", "2024-06-15"]

    def test_dict_recurses(self):
        result = deep_convert_datetimes({
            "created_at": datetime.datetime(2024, 1, 1, 12, 0),
            "due_date": datetime.date(2024, 12, 31),
            "customer_name": "Azmat",
        })
        assert result == {
            "created_at": "2024-01-01T12:00:00.000000Z",
            "due_date": "2024-12-31",
            "customer_name": "Azmat",
        }

    def test_nested_dict_in_list(self):
        result = deep_convert_datetimes([
            {"updated_at": datetime.datetime(2024, 6, 1, 9, 0)},
            {"updated_at": datetime.datetime(2024, 7, 1, 15, 30)},
        ])
        assert result[0]["updated_at"] == "2024-06-01T09:00:00.000000Z"
        assert result[1]["updated_at"] == "2024-07-01T15:30:00.000000Z"

    def test_deeply_nested_structure(self):
        result = deep_convert_datetimes({
            "order": {
                "line_items": [
                    {"shipped_at": datetime.datetime(2024, 5, 20, 8, 0)},
                ],
                "placed_on": datetime.date(2024, 5, 18),
            }
        })
        assert result["order"]["line_items"][0]["shipped_at"] == "2024-05-20T08:00:00.000000Z"
        assert result["order"]["placed_on"] == "2024-05-18"

    def test_datetime_before_date_check(self):
        """datetime is a subclass of date; verify datetime branch fires first."""
        dt = datetime.datetime(2024, 1, 1, 0, 0)
        assert isinstance(dt, datetime.date)
        result = deep_convert_datetimes(dt)
        assert "T" in result
