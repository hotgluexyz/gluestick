from gluestick.reader import Reader
from gluestick.utils.polars_utils import map_pd_type_to_polars, cast_lf_from_schema
from gluestick.snapshot_lock import prepare_snapshot_write, finish_snapshot_write
import pyarrow.parquet as pq
import polars as pl
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
        """Scan a CSV into a LazyFrame, optionally applying catalog types.

        When catalog_types is enabled, column names are read from the file header,
        then a full-column scan schema is built from the catalog. Date-time fields
        (parse_dates) are kept as strings; other catalog types are applied via
        cast_lf_from_schema. A complete schema is required because scan_csv(schema=)
        must list every column, unlike read_csv(schema_overrides=).
        """
        if catalog_types:
            catalog = self.read_catalog()
            if catalog:
                headers = pl.scan_csv(filepath, infer_schema_length=0).collect_schema().names()
                type_information = super().get_types_from_catalog(
                    catalog, stream, headers=headers
                )
                dtype = type_information.get("dtype", {})
                parse_dates = type_information.get("parse_dates", [])
                if dtype or parse_dates:
                    scan_schema = {}
                    for col in headers:
                        if col in parse_dates:
                            # Keep date-time cols as strings; cast_lf_from_schema skips parse_dates (same as parquet).
                            scan_schema[col] = pl.String
                        elif col in dtype:
                            scan_schema[col] = map_pd_type_to_polars(dtype[col])
                        else:
                            # Column in the file but not in the catalog.
                            scan_schema[col] = pl.String
                    # scan_csv schema= must include every CSV column or Polars raises a column count mismatch.
                    lf = pl.scan_csv(filepath, schema=scan_schema)
                    if dtype:
                        cast_types = {
                            col: map_pd_type_to_polars(pd_type)
                            for col, pd_type in dtype.items()
                        }
                        return cast_lf_from_schema(lf, cast_types)
                    return lf

        return pl.scan_csv(filepath)

    def get_parquet(self, stream, filepath, catalog_types=True):
        if catalog_types:
            catalog = self.read_catalog()
            if catalog:
                headers = pq.read_schema(filepath).names
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
        stream_data: pl.LazyFrame | None,
        stream: str,
        snapshot_dir: str,
        pk: str | list[str] = "id", 
        just_new: bool = False, 
        use_csv: bool = False, 
        overwrite: bool = False,
        row_group_size: int | None = None,
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
        row_group_size: int | None
            The row group size to use for the snapshot parquet file.
            If None, the row group size will be determined by the polars default.

        Returns
        -------
        return: pl.LazyFrame
            A polars lazyframe with the merged data.

        """

        if isinstance(pk, str):
            pk = [pk]

        snapshot_lf = self.read_snapshots(stream, snapshot_dir)
        if not overwrite and stream_data is not None and snapshot_lf is not None:
            snapshot_lf = snapshot_lf.join(
                stream_data.select(pk),
                on=pk,
                how="anti"
            )

            merged_lf = pl.concat(items=[snapshot_lf, stream_data], how="diagonal_relaxed")

            if use_csv:
                merged_lf.sink_csv(f"{snapshot_dir}/{stream}.temp.snapshot.csv")
                os.remove(f"{snapshot_dir}/{stream}.snapshot.csv")
                os.rename(f"{snapshot_dir}/{stream}.temp.snapshot.csv", f"{snapshot_dir}/{stream}.snapshot.csv")
            else:
                merged_lf.sink_parquet(
                    f"{snapshot_dir}/{stream}.temp.snapshot.parquet",
                    compression="zstd",
                    compression_level=3,
                    row_group_size=row_group_size,
                )
                os.remove(f"{snapshot_dir}/{stream}.snapshot.parquet")
                os.rename(f"{snapshot_dir}/{stream}.temp.snapshot.parquet", f"{snapshot_dir}/{stream}.snapshot.parquet")
            

            if just_new:
                return stream_data
            else:
                return merged_lf
        elif stream_data is not None:
            if use_csv:
                canonical_path = f"{snapshot_dir}/{stream}.snapshot.csv"
            else:
                canonical_path = f"{snapshot_dir}/{stream}.snapshot.parquet"
            lock_path = prepare_snapshot_write(canonical_path)
            if use_csv:
                stream_data.sink_csv(lock_path)
            else:
                stream_data.sink_parquet(
                    lock_path,
                    compression="zstd",
                    compression_level=3,
                    row_group_size=row_group_size,
                )
            finish_snapshot_write(lock_path, canonical_path)
            return stream_data
        elif snapshot_lf is not None:
            return snapshot_lf
        else:
            return None
