from gluestick.reader import Reader
from gluestick.utils.polars_utils import map_pd_type_to_polars, cast_lf_from_schema
import pyarrow.parquet as pq
import polars as pl
import pandas as pd
import os
class PLLazyFrameReader(Reader):

    def get(self, stream, default=None, catalog_types=True) -> pl.LazyFrame | None:
        """
        Reads the given stream from sync output and returns a pl.LazyFrame.

        Parameters
        ----------
        stream: str
            The name of the stream to read.
        default: pl.LazyFrame | None
            The default value to return if the stream is not found.
        catalog_types: bool
            Whether to coerce the lazyframe to the types given by the local catalog.
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
                    return pl.scan_csv(filepath, schema=types_params)

        return pl.scan_csv(filepath)

    def get_parquet(self, stream, filepath, catalog_types=True):
        if catalog_types:
            catalog = self.read_catalog()
            if catalog:
                headers = pq.read_table(filepath).to_pandas(safe=False).columns.tolist()
                types_params = self.get_types_from_catalog(catalog, stream, headers=headers)
                lf = pl.scan_parquet(filepath)
                return cast_lf_from_schema(lf, types_params)

        return pl.scan_parquet(filepath)
            

    def get_types_from_catalog(self, catalog, stream, headers=None):
        """Get the polars types base on the catalog definition."""
        type_information = super().get_types_from_catalog(catalog, stream, headers)
        pd_types = type_information.get("dtype", {})
        date_fields = type_information.get("parse_dates", [])
        pd_types = {
            k: "Datetime" 
            if k in date_fields 
            else v
            for k,v in pd_types.items()
            }
        return {col: map_pd_type_to_polars(pd_type) for col, pd_type in pd_types.items()}

    def read_snapshots(self,stream, snapshot_dir, **kwargs) -> pl.LazyFrame | None:
        """Read a snapshot file.

        Parameters
        ----------
        stream: str
            The name of the stream to read the snapshot from.
        snapshot_dir: str
            The path to the snapshot directory.
        """
        if os.path.isfile(path=f"{snapshot_dir}/{stream}.snapshot.parquet"):
            return pl.scan_parquet(source=f"{snapshot_dir}/{stream}.snapshot.parquet")
        elif os.path.isfile(path=f"{snapshot_dir}/{stream}.snapshot.csv"):
            return pl.scan_csv(source=f"{snapshot_dir}/{stream}.snapshot.csv")
        else:
            return None

    def snapshot_records(
        self,
        stream_data,
        stream,
        snapshot_dir,
        pk="id", 
        just_new=False, 
        use_csv=False, 
        overwrite=False, 
    ) -> pl.LazyFrame | None:
        """Update a snapshot file and return the merged data.

        Parameters
        ----------
        stream_data: pl.LazyFrame
            The data to be included in the snapshot.
        stream: str
            The name of the stream of the snapshots.
        snapshot_dir: str
            The name of the stream of the snapshots.
        pk: str or list of str
            The primary key used for the snapshot.
        just_new: bool
            Return just the input data if True, else returns the whole data
        use_csv: bool
            Whether to use csv format for the snapshot instead of parquet.
        overwrite: bool
            Whether to overwrite the existing snapshot file instead of updating and merging.

        Returns
        -------
        return: pl.LazyFrame
            A polars lazyframe with the merged data.

        """

        if isinstance(pk, str):
            pk = [pk]

        snapshot_lf = self.read_snapshots(stream, snapshot_dir)
        if not overwrite and stream_data is not None and snapshot_lf is not None:
            
            for key in pk:
                new_data_pk_lf = stream_data.select(key).collect()
                snapshot_lf = snapshot_lf.filter(
                ~pl.col(key).is_in(new_data_pk_lf.get_column(key))
                )


            merged_lf = pl.concat(items=[snapshot_lf, stream_data],how="vertical_relaxed")

            if use_csv:
                merged_lf.sink_csv(f"{snapshot_dir}/{stream}.temp.snapshot.csv")
                os.remove(f"{snapshot_dir}/{stream}.snapshot.csv")
                os.rename(f"{snapshot_dir}/{stream}.temp.snapshot.csv", f"{snapshot_dir}/{stream}.snapshot.csv")
            else:
                merged_lf.sink_parquet(f"{snapshot_dir}/{stream}.temp.snapshot.parquet")
                os.remove(f"{snapshot_dir}/{stream}.snapshot.parquet")
                os.rename(f"{snapshot_dir}/{stream}.temp.snapshot.parquet", f"{snapshot_dir}/{stream}.snapshot.parquet")
            

            if just_new:
                return stream_data
            else:
                return merged_lf
        elif stream_data is not None:
            if use_csv:
                stream_data.sink_csv(f"{snapshot_dir}/{stream}.snapshot.csv")
            else:
                stream_data.sink_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet")

            return stream_data
        elif snapshot_lf is not None:
            return snapshot_lf
        else:
            return None






