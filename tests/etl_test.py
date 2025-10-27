import os
import json
import gluestick as gs
import pandas as pd
import pytest

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
        df = pd.read_csv(
            os.path.join(dirname, "data/input/json_to_cols.csv"), index_col=0
        )
        expected_df = pd.read_csv(
            os.path.join(dirname, "data/output/json_to_cols.csv"), index_col=0
        )

        # Explode
        r = gs.array_to_dict_reducer("Name", "StringValue")
        df2 = gs.explode_json_to_cols(df, "Metadata", reducer=r)
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_cols output is correct")

    def test_explode_json_to_cols_unique(self):
        print("=====")
        print("test_explode_json_to_cols_unique")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_csv(
            os.path.join(dirname, "data/input/json_to_cols_unique.csv"), index_col=0
        )
        expected_df = pd.read_csv(
            os.path.join(dirname, "data/output/json_to_cols_unique.csv"), index_col=0
        )

        # Explode
        df2 = gs.explode_json_to_cols(df, "Metadata")
        print(df2)

        assert df2.equals(expected_df)
        print("test_explode_json_to_cols_unique output is correct")

    # Run explode_json_to_rows
    def test_explode_json_to_rows(self):
        print("=====")
        print("test_explode_json_to_rows")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_csv(
            os.path.join(dirname, "data/input/json_to_rows.csv"), index_col=0
        )
        expected_df = pd.read_csv(
            os.path.join(dirname, "data/output/json_to_rows.csv"), index_col=0
        ).astype({"Line Detail.Id": "float64"})

        # Explode
        df2 = gs.explode_json_to_rows(df, "Line Detail").astype(
            {"Line Detail.Id": "float64"}
        )
        assert df2.equals(expected_df)
        print("test_explode_json_to_rows output is correct")

    def test_explode_multi(self):
        print("=====")
        print("test_explode_multi")

        # Read data
        dirname = os.path.dirname(__file__)
        df = pd.read_csv(
            os.path.join(dirname, "data/input/multi_json.csv"), index_col=0
        )
        expected_df = (
            pd.read_csv(
                os.path.join(dirname, "data/output/explode_multi.csv"), index_col=0
            )
            .pipe(lambda x: x.astype({"LineDetail.Id": "float64"}))
            .pipe(lambda x: x.sort_index(axis=1))
        )

        transformed_df = (
            df.pipe(
                gs.explode_json_to_cols,
                "Metadata",
                reducer=gs.array_to_dict_reducer("Name", "StringValue"),
            )
            .pipe(gs.explode_json_to_rows, "LineDetail")
            .pipe(lambda x: x.astype({"LineDetail.Id": "float64"}))
            .pipe(lambda x: x.sort_index(axis=1))
        )
        assert transformed_df.equals(expected_df)

        # changing order should not matter
        transformed_df = (
            df.pipe(gs.explode_json_to_rows, "LineDetail")
            .pipe(
                gs.explode_json_to_cols,
                "Metadata",
                reducer=gs.array_to_dict_reducer("Name", "StringValue"),
            )
            .pipe(lambda x: x.astype({"LineDetail.Id": "float64"}))
            .pipe(lambda x: x.sort_index(axis=1))
        )
        assert transformed_df.equals(expected_df)

        print("test_explode_multi output is correct")


    def test_to_export(self, tmp_path):
        print("=====")
        print("test_to_export")
        dir_name = os.path.dirname(__file__)
        input = gs.Reader(dir=os.path.join(dir_name, "data/input"))

        campaign_parquet_df = input.get("campaign_performance")
        campaign_csv_df = input.get("campaign_csv")

        # Define stream name and output file
        stream_name = "campaign_performance"
        output_dir = tmp_path

        true_output_data = {}
        

        singer_output_path = os.path.join(dir_name, "data/output/data.singer")
        csv_csv_output_path = os.path.join(dir_name, "data/output/campaign_performance_csv.csv")
        parquet_csv_output_path = os.path.join(dir_name, "data/output/campaign_performance_parquet.csv")

        parquet_parquet_output_path = os.path.join(dir_name, "data/output/campaign_performance_parquet.parquet")
        csv_parquet_output_path = os.path.join(dir_name, "data/output/campaign_performance_csv.parquet")
        true_output_data["singer"] = open(singer_output_path, "r").read()




        for type, df, output_csv_path, output_parquet_path in [
            ("parquet", campaign_parquet_df, parquet_csv_output_path, parquet_parquet_output_path), 
            ("csv", campaign_csv_df, csv_csv_output_path, csv_parquet_output_path)
        ]:

            # Read the output file
            singer_output_file = output_dir / "data.singer"
            if singer_output_file.exists():
                singer_output_file.unlink()
            
            # Test singer export
            gs.to_export(
                campaign_parquet_df,
                name=stream_name,
                output_dir=output_dir,
                keys=["id"]
            )

            assert singer_output_file.exists(), f"{type} -> Singer Output file {singer_output_file} does not exist."

            with open(singer_output_file, "r") as f:
                test_lines = [json.loads(line) for line in f if line.strip()]

            with open(singer_output_path, "r") as f:
                true_lines = [json.loads(line) for line in f if line.strip()]
            
            assert test_lines == true_lines, f"{type} -> Singer output is incorrect"

            # Test CSV Export
            csv_output_file = output_dir / "campaign_performance.csv"
            if csv_output_file.exists():
                csv_output_file.unlink()
            
            gs.to_export(
                df,
                name=stream_name,
                output_dir=output_dir,
                export_format="csv",
                keys=["id"]
            )

            test_output_df = pd.read_csv(csv_output_file)
            true_output_df = pd.read_csv(output_csv_path)

            assert csv_output_file.exists(), f"{type} -> CSV Output file {csv_output_file} does not exist."

            assert test_output_df.equals(true_output_df), f"{type} -> CSV output is incorrect"


            # Test parquet export
            parquet_output_file = output_dir / "campaign_performance.parquet"
            if parquet_output_file.exists():
                parquet_output_file.unlink()

            true_output_df = pd.read_parquet(path=output_parquet_path)
            
            gs.to_export(
                df,
                name=stream_name,
                output_dir=output_dir,
                export_format="parquet",
                keys=["id"]
            )

            test_output_df = pd.read_parquet(path=parquet_output_file)

            assert parquet_output_file.exists(), f"{type} -> Parquet Output file {parquet_output_file} does not exist."

            for col in test_output_df.columns:
                print("Dtype in test: ", test_output_df[col].dtype)
                print("Dtype in true: ", true_output_df[col].dtype)
                assert test_output_df[col].equals(true_output_df[col]), f"{type} -> Column {col} is incorrect"






        print("test to_export output is correct")



