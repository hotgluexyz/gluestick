"""Utilities for hotglue ETL scripts."""

import hashlib
import json
import os

import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pytz import utc
import ast
from gluestick.singer import to_singer


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

def parse_objs(x):
    """Parse a stringified dict or list of dicts.

    Notes
    -----
    This function will parse a stringified dict or list of dicts

    Parameters
    ----------
    x: str
        stringified dict or list of dicts.

    Returns
    -------
    return: dict, list
        parsed dict or list of dicts.

    """
    # if it's not a string, we just return the input
    if type(x) != str:
        return x

    try:
        return ast.literal_eval(x)
    except:
        return json.loads(x)
    

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

    Returns
    -------
    return: file
        it outputs a singer, parquet, json or csv file

    """
    # NOTE: This is meant to allow users to override the default output name for a specific stream
    if os.environ.get(f"HG_UNIFIED_OUTPUT_{name.upper()}"):
        name = os.environ[f"HG_UNIFIED_OUTPUT_{name.upper()}"]

    if output_file_prefix:
        composed_name = f"{output_file_prefix}{name}"
    else:
        composed_name = name

    if export_format == "singer":
        to_singer(data, name, output_dir, keys=keys, allow_objects=True, unified_model=unified_model, schema=schema)
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


class Reader:
    """A reader for gluestick ETL files."""

    ROOT_DIR = os.environ.get("ROOT_DIR", ".")
    INPUT_DIR = f"{ROOT_DIR}/sync-output"

    def __init__(self, dir=INPUT_DIR, root=ROOT_DIR):
        """Init the class and read directories.

        Parameters
        ----------
        dir: str
            Directory with the input data.
        root: str
            Root directory.

        """
        self.root = root
        self.dir = dir
        self.input_files = self.read_directories()

    def __dict__(self):
        return self.input_files

    def __str__(self):
        return str(list(self.input_files.keys()))

    def __repr__(self):
        return str(list(self.input_files.keys()))

    def get(self, stream, default=None, catalog_types=False, **kwargs):
        """Read the selected file."""
        filepath = self.input_files.get(stream)
        if not filepath:
            return default
        if filepath.endswith(".parquet"):
            import pyarrow.parquet as pq
            return pq.read_table(filepath).to_pandas(safe=False)
        catalog = self.read_catalog()
        if catalog and catalog_types:
            types_params = self.get_types_from_catalog(catalog, stream)
            kwargs.update(types_params)
        return pd.read_csv(filepath, **kwargs)

    def get_metadata(self, stream):
        """Get metadata from parquet file."""
        file = self.input_files.get(stream)
        if file is None:
            raise FileNotFoundError(f"There is no file for stream with name {stream}.")
        if file.endswith(".parquet"):
            return {
                k.decode(): v.decode()
                for k, v in pq.read_metadata(file).metadata.items()
            }
        return {}

    def get_pk(self, stream):
        """Get pk from parquet file."""
        metadata = self.get_metadata(stream)
        if metadata.get("key_properties"):
            return eval(metadata["key_properties"])
        else:
            return []

    def read_directories(self, ignore=[]):
        """Read all the available directories for input files.

        Parameters
        ----------
        ignore: list
            Stream names to ignore.

        Returns
        -------
        return: dict
            Dict with the name of the streams and their paths.

        """
        is_directory = os.path.isdir(self.dir)
        all_files = []
        results = {}
        if is_directory:
            for entry in os.listdir(self.dir):
                file_path = os.path.join(self.dir, entry)
                if os.path.isfile(file_path):
                    if file_path.endswith(".csv") or file_path.endswith(".parquet"):
                        all_files.append(file_path)
        else:
            all_files.append(self.dir)

        for file in all_files:
            split_path = file.split("/")
            entity_type = split_path[len(split_path) - 1].rsplit(".", 1)[0]

            if "-" in entity_type:
                entity_type = entity_type.rsplit("-", 1)[0]

            if entity_type not in results and entity_type not in ignore:
                results[entity_type] = file

        return results

    def read_catalog(self):
        """Read the catalog.json file."""
        filen_name = f"{self.root}/catalog.json"
        if os.path.isfile(filen_name):
            with open(filen_name) as f:
                catalog = json.load(f)
        else:
            catalog = None
        return catalog

    def get_types_from_catalog(self, catalog, stream):
        """Get the pandas types base on the catalog definition.

        Parameters
        ----------
        catalog: dict
            The singer catalog used on the tap.
        stream: str
            The name of the stream.

        Returns
        -------
        return: dict
            Dict with arguments to be used by pandas.

        """
        filepath = self.input_files.get(stream)
        headers = pd.read_csv(filepath, nrows=0).columns.tolist()

        streams = next(c for c in catalog["streams"] if c["stream"] == stream or c["tap_stream_id"] == stream)
        types = streams["schema"]["properties"]

        type_mapper = {"integer": "Int64", "number": float, "boolean": "boolean"}

        dtype = {}
        parse_dates = []
        for col in headers:
            col_type = types.get(col)
            if col_type:
                # if col has multiple types, use type with format if it not exists assign type object to support multiple types
                any_of_list = col_type.get("anyOf", [])
                if any_of_list:
                    type_with_format = next((col_t for col_t in any_of_list if "format" in col_t), None)
                    col_type = type_with_format if type_with_format else {"type": "object"}
                if col_type.get("format") == "date-time":
                    parse_dates.append(col)
                    continue
                if col_type.get("type"):
                    catalog_type = [t for t in col_type["type"] if t != "null"]
                    if len(catalog_type) == 1:
                        dtype[col] = type_mapper.get(catalog_type[0], "object")
                        continue
            dtype[col] = "object"

        return dict(dtype=dtype, parse_dates=parse_dates)


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
