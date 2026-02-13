import datetime
import pandas as pd
from gluestick.utils.exceptions import CustomValidationError
from pytz import utc

__all__ = ["get_model_datetime_fields", "localize_datetime"]

def get_model_datetime_fields(model):
    """Inspect a Pydantic model and return the names of all fields typed as datetime.

    Iterates over the model's ``model_fields`` and checks each field's type
    annotation (including ``Union`` and ``Annotated`` wrappers) for
    ``datetime.datetime``.

    Args:
        model: A Pydantic model **class** (not an instance) whose fields will
            be inspected.

    Returns:
        list[str]: A list of field names whose annotations resolve to
        ``datetime.datetime``.
    """
    datetime_fields = []
    for name, field in model.model_fields.items():
        annotation = field.annotation
        if hasattr(annotation, '__args__'):
            # Check if any of the args contain datetime.datetime
            for arg in annotation.__args__:
                if arg == datetime.datetime:
                    datetime_fields.append(name)
                    break
                # Handle Annotated types
                elif hasattr(arg, '__origin__') and arg.__origin__ == datetime.datetime:
                    datetime_fields.append(name)
                    break

    return datetime_fields

def localize_datetime(data, column_names, timezone="UTC"):
    """Ensure datetime values carry timezone information.

    This function operates in two modes depending on the input types:

    **DataFrame mode** (``data`` is a ``pd.DataFrame`` and ``column_names``
    is a single ``str``):
        Converts the specified column to datetime, then localizes naive
        timestamps to UTC or converts timezone-aware timestamps to UTC.
        Returns the modified column (``pd.Series``).

    **Dict / row mode** (``data`` is a ``dict`` and ``column_names`` is a
    list of field names):
        Iterates over each field and localizes its value in-place based on
        its type:

        - **str**: parsed with ``pd.to_datetime``, then localized or
          converted to *timezone*.
        - **pd.Timestamp**: localized with ``tz_localize`` if naive, left
          as-is otherwise.
        - **datetime.datetime / datetime.date**: naive values get UTC
          attached via ``replace(tzinfo=...)``.

        Returns the mutated *data* dict.

    Args:
        data (pd.DataFrame or dict): The data to modify - either a full
            DataFrame or a single record as a dictionary.
        column_names (str or list[str]): A single column name (DataFrame
            mode) or a list of field names (dict mode) to localize.
        timezone (str): Target timezone identifier (default ``"UTC"``).
            Used only in dict mode; DataFrame mode always localizes to UTC.

    Returns:
        pd.Series or dict: The localized column series (DataFrame mode) or
        the mutated record dictionary (dict mode).

    Raises:
        CustomValidationError: If any field in dict mode contains a ``NaT``
            value.
    """
    
    if isinstance(data, pd.DataFrame) and isinstance(column_names, str):
        column_name = column_names
        # Convert the column to a Pandas Timestamp object
        data[column_name] = pd.to_datetime(data[column_name], errors="coerce")
        # Localize the column to the specified timezone
        try:
            data[column_name] = data[column_name].dt.tz_localize(utc)
        except:
            data[column_name] = data[column_name].dt.tz_convert('UTC')

        return data[column_name]
    else:
        for field in column_names:
            if data.get(field):
                value = data[field]
                # Check for NaT values, which are neither None nor valid timestamps
                if value is not None and pd.isna(value):
                    error_message = f"Field '{field}' contains a NaT (Not a Time) value which is not allowed against the record: {data}"
                    raise CustomValidationError(error_message)
                if isinstance(value, str):
                    value = pd.to_datetime(value)
                    if value.tzinfo is None:
                        value = value.tz_localize(timezone)
                    else:
                        value = value.tz_convert(timezone)
                    data[field] = value
                elif isinstance(value, pd.Timestamp):
                    data[field] = value.tz_localize(timezone) if value.tzinfo is None else value
                elif isinstance(value, datetime.datetime):
                    data[field] = value.replace(tzinfo=datetime.timezone.utc) if value.tzinfo is None else value
                elif isinstance(value, datetime.date):
                    data[field] = value.replace(tzinfo=datetime.timezone.utc) if value.tzinfo is None else value
        return data
