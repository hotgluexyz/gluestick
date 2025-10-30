from gluestick.reader import Reader
from gluestick.utils.polars_utils import map_pd_type_to_polars, cast_df_from_schema
import pyarrow.parquet as pq
import polars as pl
import pandas as pd


class PLReader(Reader):

    def get(self, stream, default=None, catalog_types=True) -> pl.DataFrame | None:
        """
        Reads the given stream from sync output and returns a pl.DataFrame.

        Parameters
        ----------
        stream: str
            The name of the stream to read.
        default: pl.DataFrame | None
            The default value to return if the stream is not found.
        catalog_types: bool
            Whether to coerce the dataframe to the types given by the local catalog.
        """

        filepath = self.input_files.get(stream)
        if not filepath:
            return default

        if filepath.endswith(".parquet"):
            return self.get_parquet(stream, filepath, catalog_types)
        elif filepath.endswith(".csv"):
            return self.get_csv(stream, filepath, catalog_types)
        raise ValueError(f"Unsupported file type: {filepath}")

    def get_csv(self, stream, filepath, catalog_types=True):
        if catalog_types:
            catalog = self.read_catalog()
            if catalog:
                headers = pd.read_csv(filepath, nrows=0).columns.tolist()
                types_params = self.get_types_from_catalog(catalog, stream, headers=headers)
                if types_params:
                    return pl.read_csv(filepath, dtypes=types_params)

        return pl.read_csv(filepath)

    def get_parquet(self, stream, filepath, catalog_types=True):
        df = pl.read_parquet(filepath)
        if catalog_types:
            catalog = self.read_catalog()
            if catalog:
                headers = pq.read_table(filepath).to_pandas(safe=False).columns.tolist()
                types_params = self.get_types_from_catalog(catalog, stream, headers=headers)
                if types_params:
                    return cast_df_from_schema(df, types_params)
        return df

    def get_types_from_catalog(self, catalog, stream, headers=None):
        """Get the polars types base on the catalog definition."""
        type_information = super().get_types_from_catalog(catalog, stream, headers)
        pd_types = type_information.get("dtype", {})
        date_fields = type_information.get("parse_dates", [])
        pd_types = {
            k: "Datetime"
            if k in date_fields
            else v
            for k, v in pd_types.items()
        }
        return {col: map_pd_type_to_polars(pd_type) for col, pd_type in pd_types.items()}
