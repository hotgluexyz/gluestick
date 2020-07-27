from functools import reduce
import pandas as pd
import ast
from pandas.io.json._normalize import nested_to_record


# Renames columns in dataframe using a json format
# NOTE: Other version of this method (https://github.com/hsyyid/hotglue/blob/master/etlutils/etlutils/ETLUtils.py#L577)
# also allows for converting the types of the values
def rename(df, target_columns):
    if target_columns is not None:
        if isinstance(target_columns, list):
            return df[target_columns]
        elif isinstance(target_columns, dict):
            idx1 = pd.Index(target_columns.keys())
            idx2 = pd.Index(df.columns)
            target_column_names = idx1.intersection(idx2).array
            return df[target_column_names].rename(columns=target_columns)
    return df


# Explodes JSON column into multiple rows?
# eg. Line with a JSON object -> 'Line.SalesItemLineDetail.ItemRef.name' etc
# Is this flattening or exploding or both combined?
def explode_json_to_rows(df, column_name):
    source_columns = df.columns
    child_df = df[source_columns]
    # Explode to new rows
    child_df = child_df.explode(column_name)

    # Flatten JSON using pandas util
    def flatten(y): return pd.Series(nested_to_record(ast.literal_eval(y), sep='.'))

    # Each row is flattened
    child_df = pd.concat([child_df, child_df[column_name].apply(flatten).add_prefix(f"{column_name}.")], axis=1)

    return child_df


# Converts array [{"Name": "First", "Value": "John"}, {"Name": "Last", "Value": "Smith"}]
def explode_json_to_cols(df, column_name, **kwargs):
    # Default reducer
    reducer = kwargs.get('reducer', array_to_dict_reducer('Name', 'Value'))

    source_columns = df.columns
    child_df = df[source_columns]
    child_df = child_df.pipe(
        lambda x: x.drop(column_name, 1).join(
            x[column_name].apply(
                lambda y: pd.Series(reduce(reducer, ast.literal_eval(y), {}))
            )
        )
    )
    return child_df


# Converts an array to a dictionary?
def array_to_dict_reducer(key_prop, value_prop):
    def reducer(result, value):
        result[value[key_prop]] = value[value_prop]
        return result

    return reducer
