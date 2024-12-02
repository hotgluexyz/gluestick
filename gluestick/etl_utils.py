"""Utilities for hotglue ETL scripts."""

import hashlib
import json
import os

import pandas as pd
from datetime import datetime
from pytz import utc
from gluestick.singer import to_singer
import re
from gluestick.reader import Reader


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
            results[entity_type] = pd.read_parquet(file, use_nullable_dtypes=True)

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
        snapshot = pd.read_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet", use_nullable_dtypes=True, **kwargs)
    elif os.path.isfile(f"{snapshot_dir}/{stream}.snapshot.csv"):
        snapshot = pd.read_csv(f"{snapshot_dir}/{stream}.snapshot.csv", **kwargs)
    else:
        snapshot = None
    return snapshot


def snapshot_records(
    stream_data, stream, snapshot_dir, pk="id", just_new=False, use_csv=False, coerce_types= False, **kwargs
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
    if stream_data is not None and snapshot is not None:
        merged_data = pd.concat([snapshot, stream_data])
        merged_data = merged_data.drop_duplicates(pk, keep="last")
        # coerce snapshot types to incoming data types
        if coerce_types:
            if not stream_data.empty and not snapshot.empty:
                # Save incoming data types
                df_types = stream_data.dtypes
                snapshot_types = snapshot.dtypes
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
        # export data
        if use_csv:
            merged_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        else:
            merged_data.to_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet", index=False)
        if not just_new:
            return merged_data

    # If there is no snapshot file snapshots and return the new data
    if stream_data is not None and snapshot is None:
        if use_csv:
            stream_data.to_csv(f"{snapshot_dir}/{stream}.snapshot.csv", index=False)
        else:
            stream_data.to_parquet(f"{snapshot_dir}/{stream}.snapshot.parquet", index=False)
        return stream_data

    # If the new data is empty return snapshot
    if just_new:
        return stream_data
    else:
        return snapshot


def get_row_hash(row):
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
    row_str = "".join(row.astype(str).values).encode()
    return hashlib.md5(row_str).hexdigest()


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

    df["hash"] = df.apply(get_row_hash, axis=1)
    # If there is a snapshot file compare and filter the hash
    hash_df = None
    if os.path.isfile(f"{output_dir}/{name}.hash.snapshot.parquet"):
        hash_df = pd.read_parquet(f"{output_dir}/{name}.hash.snapshot.parquet")
    elif os.path.isfile(f"{output_dir}/{name}.hash.snapshot.csv"):
        hash_df = pd.read_csv(f"{output_dir}/{name}.hash.snapshot.csv")

    if hash_df is not None:
        pk = [pk] if not isinstance(pk, list) else pk

        if pk:
            hash_df = hash_df.drop_duplicates(subset=pk)

        if updated_flag and pk:
            updated_pk = df[pk]
            updated_pk["_updated"] = updated_pk.isin(hash_df[pk])

        df = df.merge(
            hash_df[pk + ["hash"]], on=pk + ["hash"], how="left", indicator=True
        )
        df = df[df["_merge"] == "left_only"]
        df = df.drop("_merge", axis=1)

        if updated_flag and pk:
            df = df.merge(updated_pk, on=pk, how="left")

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

def map_fields(row, mapping):
    """Maps the row values according to the mapping dict.

    Notes
    -----
    This function will iterate through all the values of a mapping dict
    and map the values from the row accordingly

    Parameters
    ----------
    row: dict or dataframe row with the values to be mapped
    mapping: dict that estabilsh how to map the fields

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
                mapped = map_fields(row, v)
                if mapped:
                    out_list.append(mapped)
            if out_list:
                output[key] = out_list
        elif isinstance(value, dict):
            mapped = map_fields(row, value)
            if mapped:
                output[key] = mapped
        elif value is not None:
            if isinstance(row.get(value), list) or not pd.isna(row.get(value)):
                output[key] = row.get(value)
    return output

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
    reserved_variables={},
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
        to_singer(data, composed_name, output_dir, keys=keys, allow_objects=True, unified_model=unified_model, schema=schema)
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


def localize_datetime(df, column_name):
    """
    Localize a Pandas DataFrame column to a specific timezone.
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to be modified.
    column_name : str
        The name of the column to be localized.
    """
    # Convert the column to a Pandas Timestamp object
    df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    # Localize the column to the specified timezone
    try:
        df[column_name] = df[column_name].dt.tz_localize(utc)
    except:
        df[column_name] = df[column_name].dt.tz_convert('UTC')

    return df[column_name]

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
