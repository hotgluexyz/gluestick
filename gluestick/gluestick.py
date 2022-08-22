import ast
import os
import json
from contextlib import redirect_stdout
from functools import reduce

import numpy as np
import pandas as pd
import singer
from pandas.io.json._normalize import nested_to_record
from singer import Transformer


def read_csv_folder(path, converters={}, index_cols={}, ignore=[]):
    """
    Convenience method to read a set of CSV files in a folder, based on the read_csv(). This method assumes that the
    files are being pulled in a stream and follow a naming convention with the stream/ entity / table name is the first
    word in the file name for example; Account-20200811T121507.csv is for an entity called "Account".

    :param path: the folder directory
    :param converters: a dictionary with an array of converters that are passed to
    read_csv, the key of the dictionary is the name of the entity.
    :param index_cols: a dictionary with an array of
    index_cols, the key of the dictionary is the name of the entity.

    ignore = [...] -> List of files to ignore

    :return: a dict of pandas.DataFrames. the keys of which are the entity names

    Examples
    --------
    IN[31]: entity_data = read_csv_folder(CSV_FOLDER_PATH, index_cols={'Invoice': 'DocNumber'},
                        converters={'Invoice': {'Line': ast.literal_eval, 'CustomField': ast.literal_eval,
                                                'Categories': ast.literal_eval}})
    IN[32]: df = entity_data['Account']
    """

    is_directory = os.path.isdir(path)
    all_files = []
    results = {}
    if is_directory:
        for entry in os.listdir(path):
            if os.path.isfile(os.path.join(path, entry)) and os.path.join(path, entry).endswith('.csv'):
                all_files.append(os.path.join(path, entry))

    else:
        all_files.append(path)

    #print(f"Collecting data for {all_files}")

    for file in all_files:
        split_path = file.split('/')
        entity_type = split_path[len(split_path) - 1].split('.csv')[0]

        if '-' in entity_type:
            entity_type = entity_type.split('-')[0]

        if entity_type not in results and entity_type not in ignore:
            #print(f"Reading file of type {entity_type} in the data file {file}")
            results[entity_type] = pd.read_csv(file, index_col=index_cols.get(entity_type),
                                               converters=converters.get(entity_type))

    return results


def json_tuple_to_cols(df, column_name, col_config={'cols': {'key_prop': 'Name', 'value_prop': 'Value'},
                                                    'look_up': {'key_prop': 'name', 'value_prop': 'value'}}):
    """
    Convert a column with a JSON tuple in it to two column (typically a name, value pair)

    Parameters
    ----------

    :param df: the data frame
    :param column_name: column with the json tuple
    :param col_config: conversion config
    :return: a modified dataframe

    Examples
    --------
     IN[51]: qb_lookup_keys = {'key_prop': 'name', 'value_prop': 'value'}
     IN[52]: invoices = json_tuple_to_cols(invoices, 'Line.DiscountLineDetail.DiscountAccountRef',
                              col_config={'cols': {'key_prop': 'Discount Details', 'value_prop': 'Discount %'},
                                          'look_up': qb_lookup_keys})
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

    df[col_config['cols']['key_prop']] = df[column_name].apply(
        lambda y: get_value(y, col_config['look_up']['key_prop']))
    df[col_config['cols']['value_prop']] = df[column_name].apply(
        lambda y: get_value(y, col_config['look_up']['value_prop']))

    return df.drop(column_name, 1)


def rename(df, target_columns):
    """
       Renames columns in DataFrame using a json format. Also allow for converting the types of the values

       Parameters
       ----------
       :param df : dataframe
       :paramtarget_columns: the columns ro rename as a dictionary

           .. versionadded:: 1.0.0
       Returns
       -------
       df - a modified data frame

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
    """
          Explodes into multiple rows and expands into columns based on a column that has an array of JSON objects in it.

           Parameters
           ----------
           :param df : the dataframe
           :param column_name: the column that has the JSON in it.
           :param drop: drop the source column in the result. Default is True

               .. versionadded:: 1.0.0
           Returns
           -------
           df - a new data frame with the JSON line expanded into columns and rows

           Examples
           --------

           IN[52]: explode_json_to_rows(df, df['Line'] )
           an example of the line would be:
           [{"Id":"1","LineNum":"1","Amount":275,"DetailType":"SalesItemLineDetail","SalesItemLineDetail":{"ItemRef":{"value":"5","name":"Rock Fountain"},"ItemAccountRef":{"value":"79","name":"Sales of Product Income"},"TaxCodeRef":{"value":"TAX","name":null}},"SubTotalLineDetail":null,"DiscountLineDetail":null},{"Id":"2","LineNum":"2","Amount":12.75,"DetailType":"SalesItemLineDetail","SalesItemLineDetail":{"ItemRef":{"value":"11","name":"Pump"},"ItemAccountRef":{"value":"79","name":"Sales of Product Income"},"TaxCodeRef":{"value":"TAX","name":null}},"SubTotalLineDetail":null,"DiscountLineDetail":null},{"Id":"3","LineNum":"3","Amount":47.5,"DetailType":"SalesItemLineDetail","SalesItemLineDetail":{"ItemRef":{"value":"3","name":"Concrete"},"ItemAccountRef":{"value":"48","name":"Landscaping Services:Job Materials:Fountains and Garden Lighting"},"TaxCodeRef":{"value":"TAX","name":null}},"SubTotalLineDetail":null,"DiscountLineDetail":null},{"Id":null,"LineNum":null,"Amount":335.25,"DetailType":"SubTotalLineDetail","SalesItemLineDetail":null,"SubTotalLineDetail":{},"DiscountLineDetail":null}]
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
            return pd.Series(nested_to_record(y, sep='.', max_level=max_level))
        else:
            return pd.Series(dtype=np.float64)

    parser = kwargs.get("parser", ast.literal_eval)
    df[column_name] = df[column_name].apply(to_list, parser=parser)

    df = df.explode(column_name)

    df = pd.concat([df, df[column_name].apply(flatten).add_prefix(f"{column_name}.")], axis=1)
    if drop:
        df.drop(column_name, axis=1, inplace=True)

    return df


def explode_json_to_cols(df: pd.DataFrame, column_name: str, **kwargs):
    """
              Converts a JSON column that has an array value such as  [{"Name": "First", "Value": "John"},
              {"Name": "Last", "Value": "Smith"}] into a data_frame with a column for each value. Note that the new series
              produced from the JSON will be de-duplicated and inner joined with the index.

               Parameters
               ----------
               :param df : the dataframe
               :param column_name: the column that has the JSON in it.
               :param reducer: a reducer that will convert the array of JSON into a panda series

                   .. versionadded:: 1.0.0
               Returns
               -------
               df - a new data frame with the JSON line expanded into columns and rows

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

    drop = kwargs.get('drop', True)
    
    if not kwargs.get('inplace'):
        df = df.copy()

    df[column_name] = df[column_name].fillna("{}")
    parser = kwargs.get("parser", ast.literal_eval)

    df[column_name] = df[column_name].apply(lambda x: parser(x) if isinstance(x, str) else x)

    cols = df[column_name].apply(lambda x: x.keys()).explode().unique().tolist()
    cols = [x for x in cols if x == x]
    default_dict = {c:np.nan for c in cols}
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
        df = df.drop(column_name, 1)

    return df


def array_to_dict_reducer(key_prop=None, value_prop=None):
    """
    Convert an array into a dictionary

    :param key_prop: property in dictionary for key
    :param value_prop: property in dictionary for value
    :return: a dictionary that has all the accumulated values

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


def to_singer_schema(input):
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


def gen_singer_header(df, allow_objects):
    header_map = dict(type=["object", "null"], properties={})

    for col in df.columns:
        dtype = df[col].dtype.__str__().lower()
        if "float" in dtype:
            header_map["properties"][col] = {"type": ["number", "null"]}
        elif "int" in dtype:
            header_map["properties"][col] = {"type": ["integer", "null"]}
        elif "bool" in dtype:
            header_map["properties"][col] = {"type": ["boolean", "null"]}
        elif "date" in dtype:
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            header_map["properties"][col] = {"format": "date-time", "type": ["string", "null"]}
        elif allow_objects:
            value = df[col].dropna()
            if value.empty:
                header_map["properties"][col] = {"type": ["string", "null"]}
                continue
            else:
                first_value = value.iloc[0]
            
            if isinstance(first_value, list):
                if len(first_value):
                    schema = dict(type=["array", "null"], items=to_singer_schema(first_value[0]))
                    header_map["properties"][col] = schema
                else:
                    header_map["properties"][col] = {"items": {"type": ["string", "null"]}, "type": ["array", "null"]}
            elif isinstance(first_value, dict):
                schema = dict(type=["object", "null"], properties={})
                for k, v in first_value.items():
                    schema["properties"][k] = to_singer_schema(v)
                header_map["properties"][col] = schema
            else:
                header_map["properties"][col] = {"type": ["string", "null"]}
        else:
            header_map["properties"][col] = {"type": ["string", "null"]}

            def check_null(x):
                try:
                    if isinstance(x, list) or isinstance(x, dict):
                        return json.dumps(x, default=str)
                    elif not pd.isna(x):
                        return str(x)
                    return x
                except:
                    return str(x)
            df[col] = df[col].apply(check_null)
    return df, header_map


def to_singer(df: pd.DataFrame, stream, output_dir, keys=[], filename="data.singer", allow_objects=False):
    """
    Convert a pandas DataFrame into a singer file

    :param df: input DataFrame
    :param stream: stream name to be used in the singer output file
    :output_dir: path to the output directory
    :keys: the primary-keys to be used
    :allow_objects: allow or not objects to the parsed, if false defaults types to str
    """
    if allow_objects:
        df = df.dropna(how="all", axis=1)
    df, header_map = gen_singer_header(df, allow_objects)
    output = os.path.join(output_dir, filename)
    mode = "a" if os.path.isfile(output) else "w"
    with open(output, mode) as f:
        with redirect_stdout(f):
            singer.write_schema(stream, header_map, keys)
            with Transformer() as transformer:
                for i, row in df.iterrows():
                    filtered_row = row.dropna().to_dict()
                    rec = transformer.transform(filtered_row, header_map)
                    singer.write_record(stream, rec)
                singer.write_state({})


def snapshot_records(stream_data, stream, snapshot_dir, pk="id"):
    """
    Update a snapshot file

    :param stream_data: input DataFrame with the data to be included in the snapshot
    :param stream: stream name to be used as snapshot file name
    :snapshot_dir: path to the snapshots directory
    :pk: the primary key
    :return: DataFrame with all the data
    """
    if os.path.isfile(f'{snapshot_dir}/{stream}.snapshot.csv') :
        snapshot = pd.read_csv(f'{snapshot_dir}/{stream}.snapshot.csv')
    else:
        snapshot = None

    if stream_data is not None and snapshot is not None:
        stream_data = pd.concat([snapshot, stream_data])
        stream_data = stream_data.drop_duplicates(pk, keep="last")
        stream_data.to_csv(f'{snapshot_dir}/{stream}.snapshot.csv', index=False)
    elif stream_data is not None and snapshot is None:
        stream_data.to_csv(f'{snapshot_dir}/{stream}.snapshot.csv', index=False)
    else:
        stream_data = snapshot
    
    return stream_data