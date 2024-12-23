"""Singer related util functions."""

import ast
import datetime
import json
import os
from contextlib import redirect_stdout

import pandas as pd
import singer
from gluestick.reader import Reader
from singer import Transformer


def gen_singer_header(df: pd.DataFrame, allow_objects: bool, schema=None, catalog_schema=False):
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

    if schema and not catalog_schema:
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
                _schema = dict(type=["array", "null"], items=to_singer_schema(new_input))
                header_map["properties"][col] = _schema
                if not new_input:
                    header_map["properties"][col] = {
                            "items": type_mapping["str"],
                            "type": ["array", "null"],
                        } 
            elif isinstance(first_value, dict):
                _schema = dict(type=["object", "null"], properties={})
                for k, v in first_value.items():
                    _schema["properties"][k] = to_singer_schema(v)
                header_map["properties"][col] = _schema
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
    
    # update schema using types from catalog and keeping extra columns not defined in catalog
    # i.e. tenant, sync_date, etc
    if catalog_schema:
        header_map["properties"].update(schema["properties"])

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


def unwrap_json_schema(schema):
    def resolve_refs(schema, defs):
        if isinstance(schema, dict):
            if '$ref' in schema:
                ref_path = schema['$ref'].split('/')
                ref_name = ref_path[-1]
                return resolve_refs(defs[ref_name], defs)
            else:
                resolved_schema = {}
                for k,v in schema.items():
                    if type(v) != list and type(v) != dict:
                        if k not in ['required', 'title']:
                            resolved_schema[k] = v
                    else:
                        resolved_schema[k] = resolve_refs(v, defs)
                return resolved_schema
        elif isinstance(schema, list):
            return [resolve_refs(item, defs) for item in schema]
        else:
            return schema

    def simplify_anyof(schema):
        if isinstance(schema, dict):
            if 'anyOf' in schema:
                types = [item.get('type') for item in schema['anyOf'] if 'type' in item]

                # Handle cases where anyOf contains more than just type definitions
                # For example, when it includes properties or other nested structures
                combined_schema = {}
                for item in schema['anyOf']:
                    for key, value in item.items():
                        combined_schema[key] = simplify_anyof(value)
                combined_schema['type'] = types
                return combined_schema
            else:
                resolved_schema = {}
                for k,v in schema.items():
                    if type(v) != list and type(v) != dict:
                        if k not in ['required,' 'title']:
                            resolved_schema[k] = v
                    else:
                        resolved_schema[k] = simplify_anyof(v)
                return resolved_schema
        elif isinstance(schema, list):
            return [simplify_anyof(item) for item in schema]
        else:
            return schema

    defs = schema.get('$defs', {})
    resolved_schema = resolve_refs(schema, defs)
    simplified_schema = simplify_anyof(resolved_schema)
    simplified_schema.pop("$defs", None)
    return simplified_schema


def deep_convert_datetimes(value):
    """Transforms all nested datetimes in a list or dict to %Y-%m-%dT%H:%M:%S.%fZ.

    Notes
    -----
    This function transforms all datetimes to %Y-%m-%dT%H:%M:%S.%fZ

    Parameters
    ----------
    value: list, dict, datetime

    Returns
    -------
    return: list or dict with all datetime values transformed to %Y-%m-%dT%H:%M:%S.%fZ

    """
    if isinstance(value, list):
        return [deep_convert_datetimes(child) for child in value]
    elif isinstance(value, dict):
        return {k: deep_convert_datetimes(v) for k, v in value.items()}
    elif isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    elif isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    return value

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


def get_catalog_schema(stream):
    """Get a df schema using the catalog.

    Parameters
    ----------
    stream: str
        Stream name in catalog.

    """
    input = Reader()
    catalog = input.read_catalog()
    schema = next(
        (str["schema"] for str in catalog["streams"] if str["stream"] == stream), None
    )
    if not schema:
        raise Exception(f"No schema found in catalog for stream {stream}")
    else:
        # keep only relevant fields
        schema = {k: v for k, v in schema.items() if k in ["type", "properties"]}
        # need to ensure every array type has an items dict or we'll have issues
        for p in schema.get("properties", dict()):
            prop = schema["properties"][p]
            if prop.get("type") == "array" or "array" in prop.get("type") and prop.get("items") is None:
                prop["items"] = dict()
    return schema


def parse_df_cols(df, schema):
    """Parse all df list and dict columns according to schema.

    Parameters
    ----------
    stream: str
        Stream name in catalog.
    schema: dict
        Schema that will be used to export the data.

    """
    for col in df.columns:
        col_type = schema["properties"].get(col, {}).get("type", [])
        if (isinstance(col_type, list) and any(
            item in ["object", "array"]
            for item in col_type
        )) or col_type in ["object", "array"]:
            df[col] = df[col].apply(lambda x: parse_objs(x))
    return df


def to_singer(
    df: pd.DataFrame,
    stream,
    output_dir,
    keys=[],
    filename="data.singer",
    allow_objects=False,
    schema=None,
    unified_model=None,
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
    catalog_schema = os.environ.get("USE_CATALOG_SCHEMA", "false").lower() == "true"
    include_all_unified_fields = os.environ.get("INCLUDE_ALL_UNIFIED_FIELDS", "false").lower() == "true" and unified_model is not None

    if allow_objects and not (catalog_schema or include_all_unified_fields):
        df = df.dropna(how="all", axis=1)

    if catalog_schema:
        # it'll allow_objects but keeping all columns
        allow_objects = True
        # get schema from catalog
        schema = get_catalog_schema(stream)
        # parse all fields that are typed as objects or lists
        df = parse_df_cols(df, schema)

    elif unified_model:
        schema = unwrap_json_schema(unified_model.model_json_schema())

    df, header_map = gen_singer_header(df, allow_objects, schema, catalog_schema)
    output = os.path.join(output_dir, filename)
    mode = "a" if os.path.isfile(output) else "w"

    with open(output, mode) as f:
        with redirect_stdout(f):
            singer.write_schema(stream, header_map, keys)
            with Transformer() as transformer:
                for _, row in df.iterrows():
                    # keep null fields for catalog_schema and include_all_unified_fields
                    if not (catalog_schema or include_all_unified_fields):
                        filtered_row = row.dropna()
                    else:
                        filtered_row = row.where(pd.notna(row), None)
                    filtered_row = filtered_row.to_dict()
                    filtered_row = deep_convert_datetimes(filtered_row)
                    rec = transformer.transform(filtered_row, header_map)
                    singer.write_record(stream, rec)
                singer.write_state({})
