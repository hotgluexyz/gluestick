import polars as pl

def map_pd_type_to_polars(type_name):
    if not isinstance(type_name, str):
        # its a pd type class
        type_name = type_name.__name__

    if type_name == "Int64":
        return pl.Int64
    elif type_name == "Float64":
        return pl.Float64
    elif type_name in ["Boolean", "bool", "boolean"]:
        return pl.Boolean
    elif type_name == "String":
        return pl.String
    elif type_name == "Datetime":
        return pl.Datetime(time_unit="ns", time_zone="UTC")
    elif type_name == "Date":
        return pl.Date
    elif type_name == "Time":
        return pl.Time
    elif type_name == "object":
        return pl.String
    elif type_name == "float":
        return pl.Float64
    elif type_name == "int":
        return pl.Int64
    else:
        raise ValueError(f"Unknown type: {type_name}")

def cast_lf_from_schema(lf: pl.LazyFrame, types_params: dict):
    return lf.with_columns([
                    pl.col(col).cast(dtype, strict=True) for col, dtype in types_params.items()
            ])

def cast_df_from_schema(df: pl.DataFrame, types_params: dict):
    return df.with_columns([
                    pl.col(col).cast(dtype, strict=True) for col, dtype in types_params.items()
            ])
