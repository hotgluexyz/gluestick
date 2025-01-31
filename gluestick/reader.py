import os
import json
import pandas as pd
from pandas.io.parsers import TextFileReader
import pyarrow as pa
import pyarrow.parquet as pq

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
            catalog = self.read_catalog()
            if catalog and catalog_types:
                try:
                    headers = pq.read_table(filepath).to_pandas(safe=False).columns.tolist()
                    types_params = self.get_types_from_catalog(catalog, stream, headers=headers)
                    dtype_dict = types_params.get('dtype')
                    parse_dates = types_params.get('parse_dates')

                    # Mapping pandas dtypes to pyarrow types
                    type_mapping = {
                        'int64': pa.int64(),
                        'float64': pa.float64(),
                        "<class 'float'>": pa.float64(),
                        'string': pa.string(),
                        'object': pa.string(),
                        'datetime64[ns]': pa.timestamp('ns'),
                        'bool': pa.bool_(),
                        'boolean': pa.bool_(),
                        # TODO: Add more mappings as needed
                    }

                    if dtype_dict:
                        # Convert dtype dictionary to pyarrow schema
                        fields = [(col, type_mapping[str(dtype).lower()]) for col, dtype in dtype_dict.items()]
                        fields.extend([(col, pa.timestamp('ns')) for col in parse_dates])
                        schema = pa.schema(fields)
                        df = pq.read_table(filepath, schema=schema).to_pandas(safe=False)
                        for col, dtype in dtype_dict.items():
                            # NOTE: bools require explicit conversion at the end because if there are empty values (NaN)
                            # pyarrow/pd defaults to convert to string
                            if str(dtype).lower() in ["bool", "boolean"]:
                                df[col] = df[col].astype('boolean')
                            elif str(dtype).lower() in ["int64"]:
                                df[col] = df[col].astype('Int64')
                            elif str(dtype).lower() in ["object", "string"]:
                                df[col] = df[col].astype("string")
                        return df
                except:
                    # NOTE: silencing errors to avoid breaking existing workflow
                    print(f"Failed to parse catalog_types for {stream}. Ignoring.")
                    pass

            return pq.read_table(filepath).to_pandas(safe=False)
        catalog = self.read_catalog()
        if catalog and catalog_types:
            types_params = self.get_types_from_catalog(catalog, stream)
            kwargs.update(types_params)
        df = pd.read_csv(filepath, **kwargs)

        # needed to handle chunked CSVs properly
        if isinstance(df, TextFileReader):
            return df, kwargs.get("parse_dates", [])

        # if a date field value is empty read_csv will read it as "object"
        # make sure all date fields are typed as date
        for date_col in kwargs.get("parse_dates", []):
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        return df

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
        """Get pk from parquet file or catalog if available."""
        key_properties = []
        if self.read_directories().get(stream).endswith(".parquet"):
            metadata = self.get_metadata(stream)
            if metadata.get("key_properties"):
                key_properties = eval(metadata["key_properties"])
        else:
            catalog = self.read_catalog()

            if catalog is not None:
                streams = next(
                    c for c in catalog["streams"] if c.get("stream") == stream
                )
                if streams.get("metadata"):
                    breadcrumb = next(
                        s for s in streams["metadata"] if not s["breadcrumb"]
                    )
                    if breadcrumb:
                        key_properties = breadcrumb.get("metadata", {}).get(
                            "table-key-properties", []
                        )
        return key_properties

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

    def get_types_from_catalog(self, catalog, stream, headers=None):
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
        if headers is None:
            headers = pd.read_csv(filepath, nrows=0).columns.tolist()

        streams = next((c for c in catalog["streams"] if c["stream"] == stream or c["tap_stream_id"] == stream), None)
        if not streams:
            return dict()
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