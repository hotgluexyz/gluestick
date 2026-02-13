"""Utilities for hotglue ETL scripts."""

import copy
import hashlib
import json
import os

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from datetime import datetime
from pytz import utc
from gluestick.singer import to_singer
import re
from gluestick.reader import Reader
import polars as pl
from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader
from gluestick.readers.pl_reader import PolarsReader
from functools import singledispatch
from gluestick.date_utils import localize_datetime

def read_csv_folder(path, converters={}, index_cols={}, ignore=[]):
    """Read a set of CSV files in a folder using read_csv().

    Notes
    -----
    This method assumes that the files are being pulled in a stream and follow a
    naming convention with the stream/ entity / table name is the first word in the
    file name for example; Account-20200811T121507.csv is for an entity called
    ``Account``.

    Parameters
    ----------
    path: str
        The folder directory
    converters: dict
        A dictionary with an array of converters that are passed to
        read_csv, the key of the dictionary is the name of the entity.
    index_cols:
        A dictionary with an array of index_cols, the key of the dictionary is the name
        of the entity.
    ignore: list
        List of files to ignore

    Returns
    -------
    return: dict
        Dict of pandas.DataFrames. the keys of which are the entity names

    Examples
    --------
    IN[31]: entity_data = read_csv_folder(
        CSV_FOLDER_PATH,
        index_cols={'Invoice': 'DocNumber'},
        converters={'Invoice': {
            'Line': ast.literal_eval,
            'CustomField': ast.literal_eval,
            'Categories': ast.literal_eval
            }}
        )
    IN[32]: df = entity_data['Account']

    """
    is_directory = os.path.isdir(path)
    all_files = []
    results = {}
    if is_directory:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry)) and os.path.join(
                path, entry
            ).endswith(".csv"):
                all_files.append(os.path.join(path, entry))

    else:
        all_files.append(path)

    for file in all_files:
        split_path = file.split("/")
        entity_type = split_path[len(split_path) - 1].rsplit(".csv", 1)[0]

        if "-" in entity_type:
            entity_type = entity_type.rsplit("-", 1)[0]

        if entity_type not in results and entity_type not in ignore:
            # print(f"Reading file of type {entity_type} in the data file {file}")
            results[entity_type] = pd.read_csv(
                file,
                index_col=index_cols.get(entity_type),
                converters=converters.get(entity_type),
            )

    return results


def read_parquet_folder(path, ignore=[]):
    """Read a set of parquet files in a folder using read_parquet().

    Notes
    -----
    This method assumes that the files are being pulled in a stream and follow a
    naming convention with the stream/ entity / table name is the first word in the
    file name for example; Account-20200811T121507.parquet is for an entity called
    ``Account``.

    Parameters
    ----------
    path: str
        The folder directory
    ignore: list
        List of files to ignore

    Returns
    -------
    return: dict
        Dict of pandas.DataFrames. the keys of which are the entity names

    Examples
    --------
    IN[31]: entity_data = read_parquet_folder(PARQUET_FOLDER_PATH)
    IN[32]: df = entity_data['Account']

    """
    is_directory = os.path.isdir(path)
    all_files = []
    results = {}
    if is_directory:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry)) and os.path.join(
                path, entry
            ).endswith(".parquet"):
                all_files.append(os.path.join(path, entry))

    else:
        all_files.append(path)

    for file in all_files:
        split_path = file.split("/")
        entity_type = split_path[len(split_path) - 1].rsplit(".parquet", 1)[0]

        if "-" in entity_type:
            entity_type = entity_type.rsplit("-", 1)[0]

        if entity_type not in results and entity_type not in ignore:
            df = pq.read_table(file, use_threads=False).to_pandas(safe=False, use_threads=False)
            # df = df.convert_dtypes()
            results[entity_type] = df

    return results


def read_snapshots(stream, snapshot_dir, **kwargs):
    """Read a snapshot file.

    Parameters
    ----------
    stream: str
        The name of the stream to extract the snapshots from.
    snapshot_dir: str
        The path for the directory where the snapshots are stored.
    **kwargs:
        Additional arguments that are passed to pandas read_csv.

    Returns
    -------
    return: pd.DataFrame
        A pandas dataframe with the snapshot data.

    """
    # Read snapshot file if it exists
    if os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.parquet"):
        snapshot = pq.read_table(f"{snapshot_dir}/{stream}.snapshot.parquet", use_threads=False).to_pandas(safe=False, use_threads=False)
        # snapshot = snapshot.convert_dtypes()
    elif os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.csv"):
        snapshot = pd.read_csv(f"{snapshot_dir}/{stream}.snapshot.csv", **kwargs)
    else:
        snapshot = None
    return snapshot


def snapshot_records(
    stream_data, stream, snapshot_dir, pk="id", just_new=False, use_csv=False, coerce_types= False, localize_datetime_types=False, overwrite=False, **kwargs
):
    """Update a snapshot file.

    Parameters
    ----------
    stream_data: str
        DataFrame with the data to be included in the snapshot.
    stream: str
        The name of the stream of the snapshots.
    snapshot_dir: str
        The name of the stream of the snapshots.
    pk: str
        The primary key used for the snapshot.
    just_new: str
        Return just the input data if True, else returns the whole data
    coerce_types: bool
        Coerces types to the stream_data types if True, else mantains current snapshot types
    localize_datetime_types: bool
        Localizes datetime columns to UTC if True, else mantains current snapshot types
    **kwargs:
        Additional arguments that are passed to pandas read_csv.

    Returns
    -------
    return: pd.DataFrame
        A pandas dataframe with the snapshot data.

    """
    # Read snapshot file if it exists
    snapshot = read_snapshots(stream, snapshot_dir, **kwargs)

    # If snapshot file and stream data exist update the snapshot
    if not overwrite and stream_data is not None and snapshot is not None:
        snapshot_types = snapshot.dtypes

        if localize_datetime_types:
            # Localize datetime columns to UTC (datetime64[ns, UTC]) if they are not already
            for column, dtype in snapshot_types.items():
                if dtype == "datetime64[ns]":
                    snapshot[column] = localize_datetime(snapshot, column)

        merged_data = pd.concat([snapshot, stream_data])
        # coerce snapshot types to incoming data types
        if coerce_types:
            if not stream_data.empty and not snapshot.empty:
                # Save incoming data types
                df_types = stream_data.dtypes
                try:
                    for column, dtype in df_types.items():
                        if dtype == 'bool':
                            merged_data[column] = merged_data[column].astype('boolean')
                        elif dtype in ["int64", "int32", "Int32", "Int64"]:
                            merged_data[column] = merged_data[column].astype("Int64")
                        else:
                            merged_data[column] = merged_data[column].astype(dtype)
                except Exception as e:
                    raise Exception(f"Snapshot failed while trying to convert field {column} from type {snapshot_types.get(column)} to type {dtype}")
        # drop duplicates
        merged_data = merged_data.drop_duplicates(pk, keep="last")
        # export data
        if use_csv:
            merged_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        else:
            merged_data.to_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet", index=False)

        if not just_new:
            return merged_data
        else:
            return stream_data

    # If there is no snapshot file snapshots and return the new data
    if stream_data is not None:
        if use_csv:
            stream_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        else:
            stream_data.to_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet", index=False)
        return stream_data

    if just_new or overwrite:
        return stream_data
    else:
        return snapshot


def get_row_hash(row, columns):
    """Update a snapshot file.

    Parameters
    ----------
    row: pd.DataSeries
        DataFrame row to create the hash from.

    Returns
    -------
    return: str
        A string with the hash for the row.

    """
    # ensure stable order
    values = []

    for col in columns:
        v = row[col]

        if (isinstance(v, list) or not pd.isna(v)) and v==v and (v not in [None, np.nan]):
            values.append(str(v))

    row_str = "".join(values)
    return hashlib.md5(row_str.encode()).hexdigest()


def drop_redundant(df, name, output_dir, pk=[], updated_flag=False, use_csv=False):
    """Drop the rows that were present in previous versions of the dataframe.

    Notes
    -----
    This function will create a hash for every row of the dataframe and snapshot it, if
    the same row was present in previous versions of the dataframe, it will be dropped.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe do be checked for duplicates
    name: str
        The name used to snapshot the hash.
    output_dir: str
        The snapshot directory to save the state in.
    pk: list, str
        Primary key(s) used to associate the state with.
    updated_flag: bool
        To create of not a column with a flag for new/updated rows for the given
        primary key.

    Returns
    -------
    return: pd.DataFrame
        Dataframe with the data after dropping the redundant rows.

    """
    df = df.copy()

    if pk:
        # PK needs to be unique, so we drop the duplicated values
        df = df.drop_duplicates(subset=pk)

    # get a sorted list of columns to build the hash
    columns = list(df.columns)
    columns.sort()

    df["hash"] = df.apply(lambda row: get_row_hash(row, columns), axis=1)
    # If there is a snapshot file compare and filter the hash
    hash_df = None
    if os.path.isfile(f"{output_dir}/{name}.hash.snapshot.parquet"):
        hash_df = pq.read_table(f"{output_dir}/{name}.hash.snapshot.parquet", use_threads=False).to_pandas(safe=False, use_threads=False)
    elif os.path.isfile(f"{output_dir}/{name}.hash.snapshot.csv"):
        hash_df = pd.read_csv(f"{output_dir}/{name}.hash.snapshot.csv")

    if hash_df is not None:
        pk = [pk] if not isinstance(pk, list) else pk

        if pk:
            hash_df = hash_df.drop_duplicates(subset=pk)

        if updated_flag and pk:
            updated_pk = df[pk].merge(hash_df[pk], on=pk, how="inner")
            updated_pk["_updated"] = True

        df = df.merge(
            hash_df[pk + ["hash"]], on=pk + ["hash"], how="left", indicator=True
        )
        df = df[df["_merge"] == "left_only"]
        df = df.drop("_merge", axis=1)

        if updated_flag and pk:
            df = df.merge(updated_pk, on=pk, how="left")
            df["_updated"] = df["_updated"].fillna(False)

    snapshot_records(df[pk + ["hash"]], f"{name}.hash", output_dir, pk, use_csv=use_csv)
    df = df.drop("hash", axis=1)
    return df

def clean_convert(input):
    """Cleans all None values from a list or dict.

    Notes
    -----
    This function will iterate through all the values of a list or dict 
    and delete all None values 

    Parameters
    ----------
    input: dict, list
        The dict or list that will be cleaned.

    Returns
    -------
    return: dict, list
        list or dict with the data after deleting all None values.

    """
    if isinstance(input, list):
        return [clean_convert(i) for i in input]
    elif isinstance(input, dict):
        output = {}
        for k, v in input.items():
            v = clean_convert(v)
            if isinstance(v, list):
                output[k] = [i for i in v if not pd.isna(i)]
            elif not pd.isna(v):
                output[k] = v
        return output
    elif isinstance(input, datetime):
        return input.isoformat()
    elif not pd.isna(input):
        return input

def clean_obj_null_values(obj):
    """Replaces all null values by None.

    Notes
    -----
    This function will replace all null values by None so other functions 
    such as explode_json_to_cols, explode_json_to_rows, etc can be used

    Parameters
    ----------
    obj: str
        stringified dict or list where null values should be replaced.

    Returns
    -------
    return: str
        str with all null values replaced.

    """
    if not pd.isna(obj):
        obj = obj.replace('null', 'None')
        return obj
    else:
        return {}


def get_index_safely(arr, index):
    """Safely retrieves an item from an list by index.

    Parameters
    ----------
    arr: list
        List of items.
    index: int
        The index position of the item

    Returns
    -------
    return: any
        The item at the specified index, or `None` if the index is out of bounds.
    """
    try:
        return arr[index]
    except:
        return None


def build_string_format_variables(
    default_kwargs=dict(), use_tenant_metadata=True, subtenant_delimiter="_"
):
    """Builds a dictionary of string format variables from multiple sources.

    Parameters
    ----------
    default_kwargs : dict
        A dictionary of default values for the format variables. Keys in this
        dictionary are reserved and cannot be overridden by tenant metadata.
    use_tenant_metadata : bool
        Whether to include variables derived from tenant metadata. If True,
        attempts to load metadata from the tenant configuration JSON file.
    subtenant_delimiter : str
        The delimiter used to split the `tenant_id` into root and sub-tenant
        components.

    Returns
    -------
    dict
        A dictionary containing the consolidated string format variables.

    """
    # Reserved keys are keys that may not be overriden by other sources of variabes (e.g., tenant metadata)
    # The keys in the "default_kwargs" are chosen to be these reserved keys
    reserved_keys = list(default_kwargs.keys())

    final_kwargs = default_kwargs.copy()

    # Build tenant metadata variable
    tenant_metadata = dict()
    if use_tenant_metadata:
        tenant_metadata_path = (
            f"{os.environ.get('ROOT')}/snapshots/tenant-config.json"
        )
        if os.path.exists(tenant_metadata_path):
            with open(tenant_metadata_path, "r") as file:
                tenant_metadata = json.load(file)
        tenant_metadata = tenant_metadata.get("hotglue_metadata") or dict()
        tenant_metadata = tenant_metadata.get("metadata") or dict()

    # Iterate over "tenant_metadata" items and only add them in the "final_kwargs" if
    # the key is not in the "reserved_keys"
    for k, v in tenant_metadata.items():
        if k in reserved_keys:
            continue

        final_kwargs[k] = v

    flow_id = os.environ.get("FLOW")
    job_id = os.environ.get("JOB_ID")
    tap = os.environ.get("TAP")
    connector = os.environ.get("CONNECTOR_ID")
    tenant_id = os.environ.get("TENANT", "")
    env_id = os.environ.get("ENV_ID")

    splitted_tenant_id = tenant_id.split(subtenant_delimiter)
    root_tenant_id = splitted_tenant_id[0]
    sub_tenant_id = get_index_safely(splitted_tenant_id, 1) or ""

    final_kwargs.update(
        {
            "tenant": tenant_id,
            "tenant_id": tenant_id,
            "root_tenant_id": root_tenant_id,
            "sub_tenant_id": sub_tenant_id,
            "env_id": env_id,
            "flow_id": flow_id,
            "job_id": job_id,
            "tap": tap,
            "connector": connector,
        }
    )

    return final_kwargs


def format_str_safely(str_to_format, **format_variables):
    """Safely formats a string by replacing placeholders with provided values.

    Notes
    -----
    - This function skips placeholders with missing or empty values in
      `format_variables`.

    Parameters
    ----------
    str_to_format : str
        The string containing placeholders to be replaced. Placeholders
        should be in the format `{key}`.
    **format_variables : dict
        Keyword arguments representing the variables to replace in the string.

    Returns
    -------
    str
        A formatted string with the placeholders replaced by their
        corresponding values.

    """
    str_output = str_to_format

    for k, v in format_variables.items():
        if not v:
            continue
        str_output = re.sub(re.compile("{" + k + "}"), v, str_output)

    return str_output


@singledispatch
def to_export(
    data,
    name, 
    output_dir, 
    keys=[], 
    unified_model=None, 
    export_format=os.environ.get("DEFAULT_EXPORT_FORMAT", "singer"), 
    output_file_prefix=os.environ.get("OUTPUT_FILE_PREFIX"), 
    schema=None, 
    stringify_objects=False, 
    reserved_variables={}
    ):
    raise NotImplementedError("to_export is not implemented for this dataframe type")


@to_export.register(pd.DataFrame)
def pandas_df_to_export(
    data,
    name,
    output_dir,
    keys=[],
    unified_model=None,
    export_format=os.environ.get("DEFAULT_EXPORT_FORMAT", "singer"),
    output_file_prefix=os.environ.get("OUTPUT_FILE_PREFIX"),
    schema=None,
    stringify_objects=False,
    reserved_variables={},
    trim_nested_nulls=False
):
    """Parse a stringified dict or list of dicts.

    Notes
    -----
    This function will export the input data to a specified format

    Parameters
    ----------
    data: dataframe
        dataframe that will be transformed to a specified format.
    name: str
        name of the output file
    output_dir: str
        path of the folder that will store the output file
    output_file_prefix: str
        prefix of the output file name if needed
    export_format: str
        format to which the dataframe will be transformed
        supported values are: singer, parquet, json and csv
    unified_model: pydantic model
        pydantic model used to generate the schema for export format
        'singer'
    schema: dict
        customized schema used for export format 'singer'
    stringify_objects: bool
        for parquet files it will stringify complex structures as arrays
        of objects
    reserved_variables: dict
        A dictionary of default values for the format variables to be used
        in the output_file_prefix.
    trim_nested_nulls: bool
        Flag for singer export to trim nulls from nested fields

    Returns
    -------
    return: file
        it outputs a singer, parquet, json or csv file

    """
    # NOTE: This is meant to allow users to override the default output name for a specific stream
    if os.environ.get(f"HG_UNIFIED_OUTPUT_{name.upper()}"):
        name = os.environ[f"HG_UNIFIED_OUTPUT_{name.upper()}"]

    if output_file_prefix:
        # format output_file_prefix with env variables
        format_variables = build_string_format_variables(
            default_kwargs=reserved_variables
        )
        output_file_prefix = format_str_safely(output_file_prefix, **format_variables)
        composed_name = f"{output_file_prefix}{name}"
    else:
        composed_name = name

    if export_format == "singer":
        # get pk
        reader = Reader()
        keys = keys or reader.get_pk(name)
        # export data as singer
        to_singer(data, composed_name, output_dir, keys=keys, allow_objects=True, unified_model=unified_model, schema=schema, trim_nested_nulls=trim_nested_nulls)
    elif export_format == "parquet":
        if stringify_objects:
            data.to_parquet(
                os.path.join(output_dir, f"{composed_name}.parquet"),
                engine="fastparquet",
            )
        else:
            data.to_parquet(os.path.join(output_dir, f"{composed_name}.parquet"))
    elif export_format == "json":
        data.to_json(f"{output_dir}/{composed_name}.json", orient="records", date_format='iso')
    elif export_format == "jsonl":
        data.to_json(f"{output_dir}/{composed_name}.jsonl", orient='records', lines=True, date_format='iso')
    else:
        data.to_csv(f"{output_dir}/{composed_name}.csv", index=False)


@to_export.register(pl.LazyFrame)
def polars_lf_to_export(
    data,
    name,
    output_dir,
    keys=[],
    unified_model=None,
    export_format=os.environ.get("DEFAULT_EXPORT_FORMAT", "singer"),
    output_file_prefix=os.environ.get("OUTPUT_FILE_PREFIX"),
    schema=None,
    stringify_objects=False,
    reserved_variables={},
):
    """Write a Polars LazyFrame to a specified format.

    Notes
    -----
    This function will export the input data to a specified format

    Parameters
    ----------
    data: Polars LazyFrame
        Polars LazyFrame that will be transformed to a specified format.
    name: str
        name of the output file
    output_dir: str
        path of the folder that will store the output file
    output_file_prefix: str
        prefix of the output file name if needed
    export_format: str
        format to which the dataframe will be transformed
        supported values are: singer, parquet, json and csv
    unified_model: pydantic model
        pydantic model used to generate the schema for export format
        'singer'
    schema: dict
        customized schema used for export format 'singer'
    stringify_objects: bool
        for parquet files it will stringify complex structures as arrays
        of objects
    reserved_variables: dict
        A dictionary of default values for the format variables to be used
        in the output_file_prefix.

    Returns
    -------
    return: file
        it outputs a singer, parquet, json or csv file

    """
    if output_file_prefix:
        # format output_file_prefix with env variables
        format_variables = build_string_format_variables(
            default_kwargs=reserved_variables
        )
        output_file_prefix = format_str_safely(output_file_prefix, **format_variables)
        composed_name = f"{output_file_prefix}{name}"
    else:
        composed_name = name

    if export_format == "singer":
        # get pk
        reader = PLLazyFrameReader()
        keys = keys or reader.get_pk(name)
        # export data as singer
        to_singer(data, composed_name, output_dir, keys=keys, allow_objects=True, unified_model=unified_model, schema=schema)
    elif export_format == "parquet":
        data.sink_parquet(os.path.join(output_dir, f"{composed_name}.parquet"))
    elif export_format == "csv":
        data.sink_csv(os.path.join(output_dir, f"{composed_name}.csv"))
    else:
        raise ValueError(f"Unsupported export format: {export_format}")


@to_export.register(pl.DataFrame)
def polars_df_to_export(
    data,
    name,
    output_dir,
    keys=[],
    unified_model=None,
    export_format=os.environ.get("DEFAULT_EXPORT_FORMAT", "singer"),
    output_file_prefix=os.environ.get("OUTPUT_FILE_PREFIX"),
    schema=None,
    stringify_objects=False,
    reserved_variables={},
):
    """Write a Polars DataFrame to a specified format.

    Notes
    -----
    This function will export the input data to a specified format

    Parameters
    ----------
    data: Polars DataFrame
        Polars DataFrame that will be transformed to a specified format.
    name: str
        name of the output file
    output_dir: str
        path of the folder that will store the output file
    output_file_prefix: str
        prefix of the output file name if needed
    export_format: str
        format to which the dataframe will be transformed
        supported values are: singer, parquet, json and csv
    unified_model: pydantic model
        pydantic model used to generate the schema for export format
        'singer'
    schema: dict
        customized schema used for export format 'singer'
    stringify_objects: bool
        Unused for polars parquet; kept for signature compatibility
    reserved_variables: dict
        A dictionary of default values for the format variables to be used
        in the output_file_prefix.

    Returns
    -------
    return: file
        it outputs a singer, parquet, csv, json or jsonl file

    """
    if output_file_prefix:
        # format output_file_prefix with env variables
        format_variables = build_string_format_variables(
            default_kwargs=reserved_variables
        )
        output_file_prefix = format_str_safely(output_file_prefix, **format_variables)
        composed_name = f"{output_file_prefix}{name}"
    else:
        composed_name = name

    if export_format == "singer":
        # get pk
        reader = PolarsReader()
        keys = keys or reader.get_pk(name)
        # export data as singer
        to_singer(data, composed_name, output_dir, keys=keys, allow_objects=True, unified_model=unified_model, schema=schema)
    elif export_format == "parquet":
        data.write_parquet(os.path.join(output_dir, f"{composed_name}.parquet"))
    elif export_format == "csv":
        data.write_csv(os.path.join(output_dir, f"{composed_name}.csv"))
    elif export_format == "json":
        data.write_json(os.path.join(output_dir, f"{composed_name}.json"))
    elif export_format == "jsonl":
        data.write_ndjson(os.path.join(output_dir, f"{composed_name}.jsonl"))
    else:
        raise ValueError(f"Unsupported export format: {export_format}")

def exception(exception, root_dir, error_message=None):
    """
    Stores an exception and a message into a file errors.txt, 
    then the executor reads the error from the txt file to showcase the right error.
    It should be used instead of raise Exception.
    Parameters:
    -----------
    exception : the exception caught in a try except code.
    root_dir : str
        The path of the roo_dir to store errors.txt
    error_message: str
        Additional message or data to make the error clearer.
    """
    if error_message:
        error = f"ERROR: {error_message}. Cause: {exception}"
    else:
        error = f"ERROR: {exception}"
    with open(f"{root_dir}/errors.txt", "w") as outfile:
        outfile.write(error)
    raise Exception(error)

def merge_id_from_snapshot(df, snapshot_dir, stream, flow_id, pk):
    """
    Merges DataFrame with target created snapshot to retrieve existing target ids.
    
    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to be modified.
    snapshot_dir : str
        The path of the snapshot directory.
    stream : str
        The name of the stream.
    flow_id : str
        The id of the flow used to create the snapshot.
    pk : str
        The name of the primary key column to output.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with the primary key column added.
    """

    # if no pk, set it to None and return
    if not pk:
        raise Exception(f"No PK found for '{stream}'. Cannot merge.")

    # if no externalId, raise an error
    if "externalId" not in df.columns:
        raise Exception(f"'externalId' missing for '{stream}'. Cannot merge.")
    
    # read snapshot
    prefix = f"{stream}_{flow_id}"
    print(f"Reading snapshot: '{prefix}'")
    snapshot_data_frame = read_snapshots(prefix, snapshot_dir)

    # if no snapshot, return dataframe
    if snapshot_data_frame is None or snapshot_data_frame.empty:
        print(f"No snapshot for '{prefix}'.")
        return df

    # get ids from snapshot
    ids = snapshot_data_frame[["InputId", "RemoteId"]].drop_duplicates(
        subset=["InputId"], keep="last"
    )

    # merge dataframe with snapshot
    merged = df.merge(
        ids,
        left_on="externalId",
        right_on="InputId",
        how="left",
        suffixes=("", "_snap"),
    )

    # rename RemoteId to pk
    if "RemoteId" in merged.columns:
        merged = merged.rename(columns={"RemoteId": pk})

    # drop InputId (not needed)
    if "InputId" in merged.columns:
        merged = merged.drop(columns=["InputId"])

    # set pk to None if not in snapshot
    if pk in merged.columns:
        merged[pk] = merged[pk].where(pd.notna(merged[pk]), None)
    print(f"Finished getting ids from snapshot for '{stream}'.")
    return merged

def read_tenant_custom_mapping(tenant_config, flow_id=None):
    """Read the tenant mapping from the tenant config.

    Parameters
    ----------
    tenant_config : dict
        The tenant config.
    """
    # read mapping from tenant config
    raw_mapping_data = tenant_config.get("hotglue_mapping", {}).get("mapping", {})
    if not raw_mapping_data:
        print("No 'hotglue_mapping.mapping' section found in tenant config.")
        return {}, {}

    custom_field_mappings = {}
    stream_name_mapping = {}

    # get flow_id from tenant config
    potential_flow_id_key = (
        list(raw_mapping_data.keys())[0]
        if len(raw_mapping_data) == 1
        else None
    )

    flow_id = flow_id or potential_flow_id_key
    raw_mapping_data = raw_mapping_data.get(flow_id)

    if not raw_mapping_data:
        print(f"No mapping found for flow_id: {flow_id}")
        return custom_field_mappings, stream_name_mapping
    
    if not isinstance(raw_mapping_data, dict):
        print(f"Unexpected structure in mapping content: Expected dict, got {type(raw_mapping_data)}")
        raise ValueError("Invalid mapping structure.")

    # process mapping
    for combined_stream_name, field_map in raw_mapping_data.items():
        try:
            # Key format is SourceStream/TargetStream
            source_stream, target_stream = combined_stream_name.split("/", 1)
            custom_field_mappings[source_stream] = field_map
            stream_name_mapping[source_stream] = target_stream
        except Exception as e:
            raise Exception(f"Error processing mapping key '{combined_stream_name}': {e}. Skipping.")
    return custom_field_mappings, stream_name_mapping

def should_map_table(model_name, config):
    """
    Checks if a table is selected for mapping.

    Args:
        model_name (str): The name of the model/table to check.
        config (dict): Configuration containing the 'selected_tables' mapping.

    Returns:
        bool: True if the table is selected, False otherwise.
    """
    should_map = config.get("selected_tables", {}).get(model_name)
    if not should_map:
        print(f"Skipping mapping for {model_name}, not selected.")
    return bool(should_map)

def pluck_fields(objects, id_field, filter_ids, target_fields, partition_key=None, partition_key_value=None):
    """
    Extracts specific attributes from objects in a list based on filter criteria.

    Parameters:
        objects (list or DataFrame): A list or dataframe of dictionaries or objects containing attributes.
        id_field (str): The attribute name representing a unique identifier for each object.
        filter_ids (str, int, list, or set): A single ID (str or int) or a list/set of IDs (str or int) to filter objects.
        target_fields (str or list): The attribute name or a list of attribute names to extract
                                    from the selected objects.
        partition_key (str or None): Optional key to further filter objects when using a DataFrame.
                                        If provided, only rows where this column equals partition_key_value will be included.
        partition_key_value: Value to match against the partition_key column. Only used when partition_key is provided.

    Returns:
        list, dict, or str:
            - If `filter_ids` is a single ID (string):
                - Returns a single value (if `target_fields` is a string).
                - Returns a single dictionary (if `target_fields` is a list of strings).
            - If `filter_ids` is a list or set:
                - Returns a list of values (if `target_fields` is a string).
                - Returns a list of dictionaries (if `target_fields` is a list of strings).

    Raises:
        ValueError: If `target_fields` is neither a string nor a list of strings.

    Example:
        input_objects = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

        # Single ID, single field
        result = pluck_fields(input_objects, "id", 1, "name")
        # Output: "Alice"

        # Single ID, multiple fields
        result = pluck_fields(input_objects, "id", 1, ["name", "age"])
        # Output: {"name": "Alice", "age": 30}

        # Multiple IDs, single field
        result = pluck_fields(input_objects, "id", [1, 3], "name")
        # Output: ["Alice", "Charlie"]

        # Multiple IDs, multiple fields
        result = pluck_fields(input_objects, "id", [1, 3], ["name", "age"])
        # Output: [{"name": "Alice", "age": 30}, {"name": "Charlie", "age": 35}]
        
        # With partition filtering (using pandas DataFrame)
        df = pd.DataFrame([
            {"id": 1, "name": "Alice", "age": 30, "dept": "HR"},
            {"id": 2, "name": "Bob", "age": 25, "dept": "IT"},
            {"id": 3, "name": "Charlie", "age": 35, "dept": "HR"}
        ])
        result = pluck_fields(df, "id", [1, 3], ["name", "age"], "dept", "HR")
        # Output: [{"name": "Alice", "age": 30}, {"name": "Charlie", "age": 35}]
    """
    # Ensure filter_ids is a set for easier filtering
    if isinstance(filter_ids, (int, str)):
        filter_ids = {filter_ids}
        is_single = True
    elif isinstance(filter_ids, (list, set)):
        filter_ids = set(filter_ids)
        is_single = False
    elif pd.isna(filter_ids):
        return None
    else:
        raise ValueError("filter_ids must be a integer, string, list, or set.")

    # Helper to extract attributes from an object
    def extract(obj):
        if isinstance(target_fields, str):
            return obj[target_fields]
        elif isinstance(target_fields, list):
            return {field: obj[field] for field in target_fields}
        else:
            raise ValueError("target_fields must be a string or a list of strings.")

    # Filter objects based on filter_ids and extract attributes
    if objects is None:
        return None
    if isinstance(objects, list):
        # TODO: add support for partition_key and partition_key_value if needed
        results = [extract(obj) for obj in objects if obj[id_field] in filter_ids]
    elif isinstance(objects, pd.DataFrame):
        # Process pandas DataFrame
        if partition_key:
            filtered_objects = objects[objects[id_field].isin(filter_ids) & (objects[partition_key] == partition_key_value)]
        else:
            filtered_objects = objects[objects[id_field].isin(filter_ids)]
        # Convert rows to dictionaries and process them
        results = [extract(row.to_dict()) for _, row in filtered_objects.iterrows()]

    # Return a single result if filter_ids was a string
    if is_single and len(results) == 1:
        return results[0] if results else None
    if not len(results):
        return None
    return results

def process_custom_fields(row):
    """
    Process a dictionary into a list of dictionaries with custom field structure: 'name' and string 'value' keys.

    Args:
        data (dict | pd.Series): Input data, either a dictionary or a list of dictionaries.

    Returns:
        list[dict]: A list of dictionaries in the format {"name": key, "value": value}.
    """
    # Convert a single dictionary to the desired structure
    if isinstance(row, dict):
        # Handle dictionary input
        return [{"name": key, "value": value} for key, value in row.items() if not pd.isna(value)]
    elif isinstance(row, pd.Series):
        # Handle DataFrame row (pd.Series) input
        return [{"name": key, "value": value} for key, value in row.to_dict().items() if not pd.isna(value)]

def pluck_fields_by_regex(row, regex_field, return_as_cf=False):
    """
    Filters a dictionary, keeping only the key-value pairs where the key matches the regex.

    Args:
        data (dict): Input dictionary to filter.
        regex (str): Regular expression to match keys.

    Returns:
        dict: A filtered dictionary with matching key-value pairs.
    """

    pattern = re.compile(regex_field)
    result = {key: value for key, value in row.items() if pattern.match(key)}
    if return_as_cf:
        result = process_custom_fields(result)
    return result

def map_fields(row, mapping, other_data={}):
    """Maps the row values according to the mapping dict.

    Notes
    -----
    This function will iterate through all the values of a mapping dict
    and map the values from the row accordingly

    Parameters
    ----------
    row: dict or dataframe row with the values to be mapped
    mapping: dict that estabilsh how to map the fields
    other_data: dict containing additional data sources that can be referenced in the mapping

    Returns
    -------
    return: dict
        dict with the mapped data.

    """
    output = {}
    for key, value in mapping.items():
        if isinstance(value, list):
            out_list = []
            for v in value:
                mapped = map_fields(row, v, other_data)
                if mapped:
                    out_list.append(mapped)
            if out_list:
                output[key] = out_list
        elif isinstance(value, dict):
            if "pick" in value:
                kwargs = copy.deepcopy(value["pick"])
                if kwargs.get("filter_ids").startswith("rec."):
                    field = kwargs["filter_ids"].replace("rec.", "")  # Extract the field part after 'rec.'
                    filter_id = row.get(field)
                    if not filter_id:
                        output[key] = None
                        continue
                    kwargs["filter_ids"] = row.get(field)
                # read data
                
                if kwargs.get("partition_key"):
                    kwargs["partition_key_value"] = row.get(kwargs["partition_key"])
                else:
                    kwargs["partition_key"] = None
                    kwargs["partition_key_value"] = None
                object_name = kwargs.pop("objects", None)
                kwargs["objects"] = other_data.get(object_name)
                if kwargs["objects"] is not None and not kwargs["objects"].empty:
                    output[key] = pluck_fields(**kwargs)
                continue
            if "pickregex" in value:
                kwargs = value["pickregex"]
                output[key] = pluck_fields_by_regex(row, **kwargs)
                continue
            mapped = map_fields(row, value, other_data)
            if mapped:
                output[key] = mapped
        elif value is not None:
            if isinstance(row.get(value), list) or not pd.isna(row.get(value)):
                output[key] = row.get(value)
    return output

def map_fields_df(df, mapping, other_data):
    """Maps the DataFrame values according to the mapping dict, optimized for large datasets.

    Parameters
    ----------
    df: pandas DataFrame with the values to be mapped
    mapping: dict that establishes how to map the fields
    other_data: dict containing additional data sources that can be referenced in the mapping

    Returns
    -------
    DataFrame: DataFrame with the mapped data
    """
    # For simple scalar mappings, we can do this efficiently
    simple_mappings = {}
    complex_mappings = {}
    
    # Split mappings into simple and complex
    for key, value in mapping.items():
        if isinstance(value, str) or pd.isna(value):
            simple_mappings[key] = value
        else:
            complex_mappings[key] = value

    for new_col, source_col in simple_mappings.items():
        if source_col in df.columns:
            df[new_col] = df[source_col].astype(object).where(pd.notna(df[source_col]), None)
    
    # Process complex mappings if any exist
    if complex_mappings:
        for key, value in complex_mappings.items():
            if isinstance(value, dict) and "pick" in value:
                pick_args = copy.deepcopy(value["pick"])
                objects_df = other_data.get(pick_args["objects"])
                if objects_df is None or objects_df.empty:
                    continue
                
                filter_ids = pick_args["filter_ids"]
                id_field = pick_args["id_field"]
                partition_key = pick_args.get("partition_key")
                
                if filter_ids.startswith("rec."):
                    # filter_ids = filter_ids.split(".")[1]
                    filter_ids = filter_ids.replace("rec.", "")  # Extract the field part after 'rec.'
                
                target_fields = pick_args["target_fields"]

                if pick_args.get("is_list", False):
                    # Handle list of IDs to list of objects
                    if partition_key:
                        df[key] = df.apply(
                            lambda row: pluck_fields(objects_df, id_field, row[filter_ids], target_fields, partition_key, row[partition_key]),
                            axis=1
                        )
                    else:
                        df[key] = df.apply(
                            lambda row: pluck_fields(objects_df, id_field, row[filter_ids], target_fields),
                            axis=1
                        )
                    continue

                if partition_key is not None:
                    objects_df = objects_df[[id_field, pick_args["target_fields"], partition_key]].drop_duplicates(subset=[id_field, partition_key], keep='last')
                    df = df.merge(
                        objects_df.rename(columns={pick_args["target_fields"]: key}),
                        left_on=[filter_ids, partition_key],
                        right_on=[id_field, partition_key],
                        how="left",
                        suffixes=('', '_y')  # Keep left (original) column names unchanged
                    )
                else:
                    objects_df = objects_df[[id_field, pick_args["target_fields"]]].drop_duplicates(subset=[id_field], keep='last')

                    df = df.merge(
                        objects_df.rename(columns={pick_args["target_fields"]: key}),
                        left_on=filter_ids,
                        right_on=id_field,
                        how="left",
                        suffixes=('', '_y')  # Keep left (original) column names unchanged
                    )
                    
                    # Drop the id_field column from objects_df that was added during merge
                    # Check for both possible column names since pandas behavior can vary
                    if f'{id_field}_y' not in df.columns and id_field in df.columns and filter_ids != id_field:
                        # If the id_field was added and it's different from the filter_ids column
                        df = df.drop(id_field, axis=1)
                    
                df[key] = df[key].astype(object).where(pd.notna(df[key]), None)

                # Drop the right-side ID column that was added during merge
                if f'{id_field}_y' in df.columns:
                    df = df.drop(f'{id_field}_y', axis=1)
                
                # Drop the right-side partition_key column if it was added during merge
                if f'{partition_key}_y' in df.columns:
                    df = df.drop(f'{partition_key}_y', axis=1)
                    
    return df
