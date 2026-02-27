import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from gluestick.singer import parse_df_cols


class TestParseDfCols:
    def test_parses_object_column_with_list_type(self):
        df = pd.DataFrame({"metadata": ['{"city": "Austin"}', '{"city": "NYC"}']})
        schema = {"properties": {"metadata": {"type": ["object", "null"]}}}
        result = parse_df_cols(df, schema)
        assert result["metadata"].iloc[0] == {"city": "Austin"}

    def test_parses_array_column_with_list_type(self):
        df = pd.DataFrame({"tags": ['["electronics", "sale"]', '["new"]']})
        schema = {"properties": {"tags": {"type": ["array", "null"]}}}
        result = parse_df_cols(df, schema)
        assert result["tags"].iloc[0] == ["electronics", "sale"]

    def test_parses_column_with_scalar_object_type(self):
        df = pd.DataFrame({"address": ['{"zip": "10001"}']})
        schema = {"properties": {"address": {"type": "object"}}}
        result = parse_df_cols(df, schema)
        assert result["address"].iloc[0] == {"zip": "10001"}

    def test_parses_column_with_scalar_array_type(self):
        df = pd.DataFrame({"items": ['[1, 2, 3]']})
        schema = {"properties": {"items": {"type": "array"}}}
        result = parse_df_cols(df, schema)
        assert result["items"].iloc[0] == [1, 2, 3]

    def test_skips_string_typed_columns(self):
        df = pd.DataFrame({"customer_name": ['{"not": "parsed"}']})
        schema = {"properties": {"customer_name": {"type": ["string", "null"]}}}
        result = parse_df_cols(df, schema)
        assert result["customer_name"].iloc[0] == '{"not": "parsed"}'

    def test_skips_columns_not_in_schema(self):
        df = pd.DataFrame({"unknown_col": ['{"key": "val"}']})
        schema = {"properties": {}}
        result = parse_df_cols(df, schema)
        assert result["unknown_col"].iloc[0] == '{"key": "val"}'

    def test_non_string_values_pass_through(self):
        df = pd.DataFrame({"metadata": [{"already": "parsed"}, None]})
        schema = {"properties": {"metadata": {"type": ["object", "null"]}}}
        result = parse_df_cols(df, schema)
        assert result["metadata"].iloc[0] == {"already": "parsed"}
        assert result["metadata"].iloc[1] is None
