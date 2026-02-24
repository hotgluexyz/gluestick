import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from gluestick.singer import combine_anyof_types


def test_combine_anyof_types_merges_unique_types_from_lists_and_strings():
    field_types = [
        {"type": "string"},
        {"type": ["null", "integer"]},
        {"type": ["string", "number"]},
        {"format": "date-time"},
    ]

    result = combine_anyof_types(field_types)

    assert result == ["integer", "null", "number", "string"]


def test_combine_anyof_types_raises_for_invalid_type_value():
    field_types = [{"type": {"not": "valid"}}]

    with pytest.raises(ValueError, match="Invalid type"):
        combine_anyof_types(field_types)
