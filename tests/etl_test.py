import pytest
import pandas as pd

import gluestick as gs


# Tests gluestick ETL utilities
class TestETL(object):

    @classmethod
    def setup_class(cls):
        print("=====")
        print("setup")

    # TODO: Test join

    # Run explode_json_to_cols
    def test_explode_json_to_cols(self):
        print("=====")
        print("test_explode_json_to_cols")

        # Read data
        df = pd.read_excel('data/input/json_to_cols.xlsx')
        expected_df = pd.read_csv('data/output/json_to_cols.csv')

        # Explode
        r = gs.array_to_dict_reducer('Name', 'StringValue')
        df2 = gs.explode_json_to_cols(df, "Metadata", reducer=r)
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_cols output is correct")

    # Run explode_json_to_rows
    def test_explode_json_to_rows(self):
        print("=====")
        print("test_explode_json_to_rows")

        # Read data
        df = pd.read_excel('data/input/json_to_rows.xlsx')
        expected_df = pd.read_csv('data/output/json_to_rows.csv')

        # Explode
        df2 = gs.explode_json_to_rows(df, "Metadata")
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_rows output is correct")
