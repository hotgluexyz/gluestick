"""Singer related util functions."""

import json
import os
from contextlib import redirect_stdout

import pandas as pd
import singer
from singer import Transformer


def gen_singer_header(df: pd.DataFrame, allow_objects: bool, schema=None):
    """Generate singer headers based on pandas types.

    Parameters
    ----------
    df: pandas.DataFrame
        The dataframe to extranct the types from.
    allow_objects: bool
        If the function should proccess objects in the columns.

    Returns
    -------
    return: dict
        Dict of pandas.DataFrames. the keys of which are the entity names

    """
    header_map = dict(type=["object", "null"], properties={})

    type_mapping = {
        "float": {"type": ["number", "null"]},
        "int": {"type": ["integer", "null"]},
        "bool": {"type": ["boolean", "null"]},
        "str": {"type": ["string", "null"]},
        "date": {
            "format": "date-time",
            "type": ["string", "null"],
        },
    }

    if schema:
        header_map = schema
        return df, header_map
    
    for col in df.columns:
        dtype = df[col].dtype.__str__().lower()

        if "date" in dtype:
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        col_type = next((t for t in type_mapping.keys() if t in dtype), None)

        if col_type:
            header_map["properties"][col] = type_mapping[col_type]
        elif allow_objects:
            value = df[col].dropna()
            if value.empty:
                header_map["properties"][col] = type_mapping["str"]
                continue
            else:
                first_value = value.iloc[0]

            if isinstance(first_value, list):
                new_input = {}
                for row in value:
                    if len(row):
                        for arr_value in row:
                            if isinstance(arr_value, dict):
                                temp_dict = {k:v for k, v in arr_value.items() if (k not in new_input.keys()) or isinstance(v, float)}
                                new_input.update(temp_dict)
                            else:
                                new_input = arr_value        
                schema = dict(type=["array", "null"], items=to_singer_schema(new_input))
                header_map["properties"][col] = schema
                if not new_input:
                    header_map["properties"][col] = {
                            "items": type_mapping["str"],
                            "type": ["array", "null"],
                        } 
            elif isinstance(first_value, dict):
                schema = dict(type=["object", "null"], properties={})
                for k, v in first_value.items():
                    schema["properties"][k] = to_singer_schema(v)
                header_map["properties"][col] = schema
            else:
                header_map["properties"][col] = type_mapping["str"]
        else:
            header_map["properties"][col] = type_mapping["str"]

            def check_null(x):
                if isinstance(x, list) or isinstance(x, dict):
                    return json.dumps(x, default=str)
                elif not pd.isna(x):
                    return str(x)
                return x

            df[col] = df[col].apply(check_null)
    return df, header_map


def to_singer_schema(input):
    """Generate singer headers based on pandas types.

    Parameters
    ----------
    input:
        Object to extract the types from.

    Returns
    -------
    return: dict
        Dict of the singer mapped types.

    """
    if type(input) == dict:
        property = dict(type=["object", "null"], properties={})
        for k, v in input.items():
            property["properties"][k] = to_singer_schema(v)
        return property
    elif type(input) == list:
        if len(input):
            return dict(type=["array", "null"], items=to_singer_schema(input[0]))
        else:
            return {"items": {"type": ["string", "null"]}, "type": ["array", "null"]}
    elif type(input) == bool:
        return {"type": ["boolean", "null"]}
    elif type(input) == int:
        return {"type": ["integer", "null"]}
    elif type(input) == float:
        return {"type": ["number", "null"]}
    return {"type": ["string", "null"]}


def to_singer(
    df: pd.DataFrame,
    stream,
    output_dir,
    keys=[],
    filename="data.singer",
    allow_objects=False,
    schema = None
):
    """Convert a pandas DataFrame into a singer file.

    Parameters
    ----------
    df: pd.DataFrame
        Object to extract the types from.
    stream: str
        Stream name to be used in the singer output file.
    output_dir: str
        Path to the output directory.
    keys: list
        The primary-keys to be used.
    filename: str
        The output file name.
    allow_objects: boolean
        Allow or not objects to the parsed, if false defaults types to str.

    """
    if allow_objects:
        df = df.dropna(how="all", axis=1)
    df, header_map = gen_singer_header(df, allow_objects, schema)
    output = os.path.join(output_dir, filename)
    mode = "a" if os.path.isfile(output) else "w"

    with open(output, mode) as f:
        with redirect_stdout(f):
            singer.write_schema(stream, header_map, keys)
            with Transformer() as transformer:
                for i, row in df.iterrows():
                    filtered_row = row.dropna().to_dict()
                    for key, value in filtered_row.items():
                        if isinstance(value, pd.Timestamp):
                            filtered_row[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    rec = transformer.transform(filtered_row, header_map)
                    singer.write_record(stream, rec)
                singer.write_state({})
