import pytest
import os
import pandas as pd

import gluestick


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
        dirname = os.path.dirname(__file__)
        df = pd.read_excel(os.path.join(dirname, 'data/input/json_to_cols.xlsx'), index_col=0)
        expected_df = pd.read_csv(os.path.join(dirname, 'data/output/json_to_cols.csv'), index_col=0)

        # Explode
        r = gluestick.array_to_dict_reducer('Name', 'StringValue')
        df2 = gluestick.explode_json_to_cols(df, "Metadata", reducer=r)
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_cols output is correct")

    def test_explode_json_to_cols_unique(self):
        print("=====")
        print("test_explode_json_to_cols_unique")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_excel(os.path.join(dirname, 'data/input/json_to_cols_unique.xlsx'), index_col=0)
        expected_df = pd.read_csv(os.path.join(dirname, 'data/output/json_to_cols_unique.csv'), index_col=0)

        # Explode
        df2 = gluestick.explode_json_to_cols(df, "Metadata")
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_cols_unique output is correct")

    # Run explode_json_to_rows
    def test_explode_json_to_rows(self):
        print("=====")
        print("test_explode_json_to_rows")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_excel(os.path.join(dirname, 'data/input/json_to_rows.xlsx'), index_col=0)
        expected_df = pd.read_csv(os.path.join(dirname, 'data/output/json_to_rows.csv'), index_col=0).astype(
            {'Line Detail.Id': 'float64'})

        # Explode
        df2 = gluestick.explode_json_to_rows(df, "Line Detail").astype({'Line Detail.Id': 'float64'})
        assert df2.equals(expected_df)
        print("test_explode_json_to_rows output is correct")

    def test_explode_multi(self):
        print("=====")
        print("test_explode_multi")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_excel(os.path.join(dirname, 'data/input/multi_json.xlsx'), index_col=0)
        expected_df = (pd.read_csv(os.path.join(dirname, 'data/output/explode_multi.csv'), index_col=0)
                       .pipe(lambda x: x.astype({'LineDetail.Id': 'float64'}))
                       .pipe(lambda x: x.sort_index(axis=1))
                       )

        transformed_df = (df
                          .pipe(gluestick.explode_json_to_cols, "Metadata",
                                reducer=gluestick.array_to_dict_reducer('Name', 'StringValue'))
                          .pipe(gluestick.explode_json_to_rows, "LineDetail")
                          .pipe(lambda x: x.astype({'LineDetail.Id': 'float64'}))
                          .pipe(lambda x: x.sort_index(axis=1))
                          )
        assert transformed_df.equals(expected_df)

        # changing order should not matter
        transformed_df = (df
                          .pipe(gluestick.explode_json_to_rows, "LineDetail")
                          .pipe(gluestick.explode_json_to_cols, "Metadata",
                                reducer=gluestick.array_to_dict_reducer('Name', 'StringValue'))
                          .pipe(lambda x: x.astype({'LineDetail.Id': 'float64'}))
                          .pipe(lambda x: x.sort_index(axis=1))
                          )
        assert transformed_df.equals(expected_df)

        print("test_explode_multi output is correct")
