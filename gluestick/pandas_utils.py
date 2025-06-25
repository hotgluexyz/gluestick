"""Utilities for pandas dataframes containing objects."""

import ast

import numpy as np
import pandas as pd
from pandas.io.json._normalize import nested_to_record


def json_tuple_to_cols(
    df,
    column_name,
    col_config={
        "cols": {"key_prop": "Name", "value_prop": "Value"},
        "look_up": {"key_prop": "name", "value_prop": "value"},
    },
):
    """Convert a column with a JSON tuple in it to two column.

    Parameters
    ----------
    df: pd.DataFrame
        The input pandas data frame.
    column_name: str
        Column with the json tuple.
    col_config:
        Conversion config.

    Returns
    -------
    return: pd.DataFrame
        A dataframe with the new columns.

    Examples
    --------
     IN[51]: qb_lookup_keys = {'key_prop': 'name', 'value_prop': 'value'}
     IN[52]: invoices = json_tuple_to_cols(
        invoices,
        'Line.DiscountLineDetail.DiscountAccountRef',
        col_config={
            'cols': {
                'key_prop': 'Discount Details',
                'value_prop': 'Discount %'
                },
            'look_up': qb_lookup_keys
            }
        )

    """

    def get_value(y, prop):
        value = y
        if type(value) is str:
            value = ast.literal_eval(y)
        if type(value) is dict:
            return value.get(prop)
        if type(value) is list:
            return value[0].get(prop)
        else:
            return None

    df[col_config["cols"]["key_prop"]] = df[column_name].apply(
        lambda y: get_value(y, col_config["look_up"]["key_prop"])
    )
    df[col_config["cols"]["value_prop"]] = df[column_name].apply(
        lambda y: get_value(y, col_config["look_up"]["value_prop"])
    )

    return df.drop(column_name, 1)


def rename(df, target_columns):
    """Rename columns in DataFrame using a json format.

    Notes
    -----
    Also allow for converting the types of the values.

    Parameters
    ----------
    df: pd.DataFrame
        The input pandas data frame.
    target_columns: dict
        Dictionary with the columns to rename.

    Returns
    -------
    return: pd.DataFrame
        Modified data frame with the renamed columns.

    Examples
    --------
    IN[52]: rename(df, )
    Out[52]:
    {'dict1.c': 1,
     'dict1.d': 2,
     'flat1': 1,
     'nested.d': 2,
     'nested.e.c': 1,
     'nested.e.d': 2}

    """
    if target_columns is not None:
        if isinstance(target_columns, list):
            return df[target_columns]
        elif isinstance(target_columns, dict):
            idx1 = pd.Index(target_columns.keys())
            idx2 = pd.Index(df.columns)
            target_column_names = idx1.intersection(idx2).array
            return df[target_column_names].rename(columns=target_columns)
    return df


def explode_json_to_rows(df, column_name, drop=True, **kwargs):
    """Explodes a column with an array of objects into multiple rows.

    Notes
    -----
    Convert an array of objects into a list of dictionaries and explode it into
    multiple rows and columns, one column for each dictionary key and one row for each
    object inside the array.

    Parameters
    ----------
    df: pd.DataFrame
        The input pandas data frame.
    column_name: str
        The name of the column that should be exploded.
    drop: boolean
        To drop or not the exploded column.
    **kwargs:
        Additional arguments.


    Returns
    -------
    return: pd.DataFrame
        New data frame with the JSON line expanded into columns and rows.

    Examples
    --------
    IN[52]: explode_json_to_rows(df, df['Line'] )
    an example of the line would be:
    [
        {
            "Id":"1",
            "LineNum":"1",
            "Amount":275,
            "DetailType":"SalesItemLineDetail",
            "SalesItemLineDetail":{"ItemRef":{"value":"5","name":"Rock Fountain"},
            "ItemAccountRef":{"value":"79","name":"Sales of Product Income"},
            "TaxCodeRef":{"value":"TAX","name":null},
            "SubTotalLineDetail":null,
            "DiscountLineDetail":null
        },
        {
            "Id":"2",
            "LineNum":"2",
            "Amount":12.75,
            "DetailType":"SalesItemLineDetail",
            "SalesItemLineDetail":{"ItemRef":{"value":"11","name":"Pump"},
            "ItemAccountRef":{"value":"79","name":"Sales of Product Income"},
            "TaxCodeRef":{"value":"TAX","name":null},
            "SubTotalLineDetail":null,
            "DiscountLineDetail":null
        },
        {
            "Id":"3",
            "LineNum":"3",
            "Amount":47.5,
            "DetailType":"SalesItemLineDetail",
            "SalesItemLineDetail":{"ItemRef":{"value":"3","name":"Concrete"},
            "ItemAccountRef":{
                "value":"48",
                "name":"Landscaping Services:Job Materials"
                },
            "TaxCodeRef":{"value":"TAX","name":null},
            "SubTotalLineDetail":null,
            "DiscountLineDetail":null
        },
        {
            "Id":null,
            "LineNum":null,
            "Amount":335.25,
            "DetailType":"SubTotalLineDetail",
            "SalesItemLineDetail":null,
            "SubTotalLineDetail":{},
            "DiscountLineDetail":null
        }
    ]
    Out[52]:
    Line.Id Line.LineNum  Line.Amount      Line.DetailType
    Index
    1037            1            1       275.00  SalesItemLineDetail
    1037            2            2        12.75  SalesItemLineDetail
    1037            3            3        47.50  SalesItemLineDetail
    1037         None         None       335.25   SubTotalLineDetail
    1036            1            1        50.00  SalesItemLineDetail

    """
    # Explode to new rows
    max_level = kwargs.get("max_level", 1)

    def to_list(y, parser=ast.literal_eval):
        if type(y) is str:
            y = parser(y)

        if type(y) is not list:
            y = [y]

        return y

    def flatten(y):
        if type(y) is dict:
            return pd.Series(nested_to_record(y, sep=".", max_level=max_level))
        else:
            return pd.Series(dtype=np.float64)

    parser = kwargs.get("parser", ast.literal_eval)
    df[column_name] = df[column_name].apply(to_list, parser=parser)

    df = df.explode(column_name)

    df = pd.concat(
        [df, df[column_name].apply(flatten).add_prefix(f"{column_name}.")], axis=1
    )
    if drop:
        df.drop(column_name, axis=1, inplace=True)

    return df


def explode_json_to_cols(df: pd.DataFrame, column_name: str, **kwargs):
    """Convert a JSON column that has an array value into a DataFrame.

    Notes
    -----
    Arrays such as [{"Name": "First", "Value": "Jo"},{"Name": "Last", "Value": "Do"}]
    with a column for each value are converted to pandas DataFrame. Note that the new
    series produced from the JSON will be de-duplicated and inner joined with the
    index.

    Parameters
    ----------
    df: pd.DataFrame
        The input pandas data frame.
    column_name: str
        The name of the column that should be exploded.
    **kwargs:
        Additional arguments.

    Returns
    -------
    return: pd.DataFrame
        New data frame with the JSON line expanded into columns.

    Examples
    --------
    IN[5]: explode_json_to_cols(df, 'ProductRef' )
    an example of the ProductRef would be:
    {"value": "Hi Tea Chipper","name": "Product"},
    Out[5]:
    Product
    Index
    1037       Hi Tea Chipper

    """
    drop = kwargs.get("drop", True)

    if not kwargs.get("inplace"):
        df = df.copy()

    df[column_name] = df[column_name].fillna("{}")
    parser = kwargs.get("parser", ast.literal_eval)

    df[column_name] = df[column_name].apply(
        lambda x: parser(x) if isinstance(x, str) else x
    )

    cols = df[column_name].apply(lambda x: x.keys()).explode().unique().tolist()
    cols = [x for x in cols if x == x]
    default_dict = {c: np.nan for c in cols}
    cols = [f"{column_name}.{col}" for col in cols]

    def set_default_dict(object, default_dict):
        if isinstance(object, dict):
            for k, v in default_dict.items():
                object.setdefault(k, v)
            return object
        return np.nan

    df[column_name] = df[column_name].apply(lambda x: set_default_dict(x, default_dict))
    df[cols] = df[column_name].apply(pd.Series)

    if drop:
        df = df.drop(column_name, axis=1)

    return df


def array_to_dict_reducer(key_prop=None, value_prop=None):
    """Convert an array into a dictionary.

    Parameters
    ----------
    key_prop: str
        Property in dictionary for key.
    value_prop: str
        Property in dictionary for value.

    Returns
    -------
    return: dict
        A dictionary that has all the accumulated values.

    """

    def reducer(accumulator, current_value):
        if type(current_value) is not dict:
            raise AttributeError("Value being reduced must be a dictionary")

        if key_prop is not None and value_prop is not None:
            key = current_value.get(key_prop)
            current_value = current_value.get(value_prop)
            accumulator[key] = current_value
        else:
            for key, value in current_value.items():
                accumulator[key] = value

        return accumulator

    return reducer


def compress_rows_to_col(df: pd.DataFrame, column_prefix: str, pk):
    """Compress exploded columns rows back to a single column.

    Parameters
    ----------
    df: pd.DataFrame
        Input DataFrame to be compressed.
    column_prefix: str
        Column prefix to be compressed.
    pk: str
        Primary key to group on.

    Returns
    -------
    return: pd.DataFrame
        A data frame with the compressed data.

    """
    compress_cols = [col for col in df.columns if col.startswith(column_prefix)]
    df_compress = df[compress_cols]
    df.drop(compress_cols, inplace=True, axis=1)

    prefix_len = len(column_prefix) + 1
    cols_rename = {c: c[prefix_len:] for c in compress_cols}
    df_compress.rename(cols_rename, axis=1, inplace=True)

    df[column_prefix] = df_compress.apply(lambda x: str(x.to_dict()), axis=1)

    grouped = df.groupby(pk, axis=0)[column_prefix].apply(list).reset_index()
    df.drop_duplicates(pk, inplace=True)
    return df.merge(grouped, how="left", on=pk)
