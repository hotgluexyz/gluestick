import datetime
import pandas as pd
import pytz
from gluestick.date_utils import localize_datetime
from gluestick.utils.exceptions import CustomValidationError


# -------------------------------------------------------------------
# DataFrame mode tests
# -------------------------------------------------------------------

def test_df_naive_column_is_localized_to_utc():
    """DataFrame with a naive datetime column gets localized to UTC."""
    print("=====")
    print("test_df_naive_column_is_localized_to_utc")

    df = pd.DataFrame({"created_at": ["2024-06-15 10:30:00", "2024-07-01 08:00:00"]})
    result = localize_datetime(df, "created_at")

    assert result.dtype == "datetime64[ns, UTC]"
    assert result.dt.tz is not None
    print("test_df_naive_column_is_localized_to_utc output is correct")


def test_df_aware_column_is_converted_to_utc():
    """DataFrame with a tz-aware column gets converted to UTC."""
    print("=====")
    print("test_df_aware_column_is_converted_to_utc")

    eastern = pytz.timezone("US/Eastern")
    ts = pd.Timestamp("2024-06-15 10:30:00", tz=eastern)
    df = pd.DataFrame({"created_at": [ts]})

    result = localize_datetime(df, "created_at")

    assert str(result.dt.tz) == "UTC"
    expected_utc = ts.tz_convert("UTC")
    assert result.iloc[0] == expected_utc
    print("test_df_aware_column_is_converted_to_utc output is correct")


def test_df_coerces_invalid_strings_to_nat():
    """Invalid date strings are coerced to NaT (errors='coerce')."""
    print("=====")
    print("test_df_coerces_invalid_strings_to_nat")

    df = pd.DataFrame({"created_at": ["not-a-date", "2024-06-15"]})
    result = localize_datetime(df, "created_at")

    assert pd.isna(result.iloc[0])
    assert not pd.isna(result.iloc[1])
    print("test_df_coerces_invalid_strings_to_nat output is correct")


def test_df_returns_series():
    """DataFrame mode returns a pd.Series (the modified column)."""
    print("=====")
    print("test_df_returns_series")

    df = pd.DataFrame({"ts": ["2024-01-01"]})
    result = localize_datetime(df, "ts")

    assert isinstance(result, pd.Series)
    print("test_df_returns_series output is correct")


# -------------------------------------------------------------------
# Dict mode - string values
# -------------------------------------------------------------------

def test_dict_naive_string_is_localized():
    """Naive datetime string in a dict gets localized to the target timezone."""
    print("=====")
    print("test_dict_naive_string_is_localized")

    row = {"created_at": "2024-06-15 10:30:00", "name": "Alice"}
    result = localize_datetime(row, ["created_at"], timezone="US/Eastern")

    assert isinstance(result["created_at"], pd.Timestamp)
    assert str(result["created_at"].tzinfo) == "US/Eastern"
    print("test_dict_naive_string_is_localized output is correct")


def test_dict_aware_string_is_converted():
    """Tz-aware datetime string gets converted to the target timezone."""
    print("=====")
    print("test_dict_aware_string_is_converted")

    row = {"created_at": "2024-06-15T10:30:00+00:00"}
    result = localize_datetime(row, ["created_at"], timezone="US/Eastern")

    assert result["created_at"].tzinfo is not None
    # Original UTC 10:30 → Eastern should be 06:30
    assert result["created_at"].hour == 6
    print("test_dict_aware_string_is_converted output is correct")


def test_dict_string_defaults_to_utc():
    """When no timezone is given, naive string defaults to UTC."""
    print("=====")
    print("test_dict_string_defaults_to_utc")

    row = {"ts": "2024-01-15 12:00:00"}
    result = localize_datetime(row, ["ts"])

    assert str(result["ts"].tzinfo) == "UTC"
    print("test_dict_string_defaults_to_utc output is correct")


# -------------------------------------------------------------------
# Dict mode - pd.Timestamp values
# -------------------------------------------------------------------

def test_dict_naive_timestamp_is_localized():
    """Naive pd.Timestamp gets localized to the target timezone."""
    print("=====")
    print("test_dict_naive_timestamp_is_localized")

    row = {"updated_at": pd.Timestamp("2024-06-15 14:00:00")}
    result = localize_datetime(row, ["updated_at"], timezone="US/Pacific")

    assert result["updated_at"].tzinfo is not None
    assert str(result["updated_at"].tzinfo) == "US/Pacific"
    print("test_dict_naive_timestamp_is_localized output is correct")


def test_dict_aware_timestamp_is_unchanged():
    """Already tz-aware pd.Timestamp is left as-is."""
    print("=====")
    print("test_dict_aware_timestamp_is_unchanged")

    ts = pd.Timestamp("2024-06-15 14:00:00", tz="US/Eastern")
    row = {"updated_at": ts}
    result = localize_datetime(row, ["updated_at"], timezone="US/Pacific")

    # Should remain Eastern, not re-localized to Pacific
    assert str(result["updated_at"].tzinfo) == "US/Eastern"
    assert result["updated_at"] == ts
    print("test_dict_aware_timestamp_is_unchanged output is correct")


# -------------------------------------------------------------------
# Dict mode - datetime.datetime values
# -------------------------------------------------------------------

def test_dict_naive_datetime_gets_utc():
    """Naive datetime.datetime gets UTC attached."""
    print("=====")
    print("test_dict_naive_datetime_gets_utc")

    dt = datetime.datetime(2024, 6, 15, 10, 30, 0)
    row = {"event_time": dt}
    result = localize_datetime(row, ["event_time"])

    assert result["event_time"].tzinfo == datetime.timezone.utc
    print("test_dict_naive_datetime_gets_utc output is correct")


def test_dict_aware_datetime_is_unchanged():
    """Already tz-aware datetime.datetime is left as-is."""
    print("=====")
    print("test_dict_aware_datetime_is_unchanged")

    dt = datetime.datetime(2024, 6, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
    row = {"event_time": dt}
    result = localize_datetime(row, ["event_time"])

    assert result["event_time"] is dt  # exact same object
    print("test_dict_aware_datetime_is_unchanged output is correct")


# -------------------------------------------------------------------
# Dict mode - datetime.date values
# -------------------------------------------------------------------

def test_dict_naive_date_raises_attribute_error():
    """Naive datetime.date hits a bug — datetime.date has no tzinfo attribute.

    Known issue: the datetime.date branch in localize_datetime tries to
    access ``value.tzinfo``, but ``datetime.date`` objects do not have that
    attribute (only ``datetime.datetime`` does).
    """
    print("=====")
    print("test_dict_naive_date_raises_attribute_error")

    d = datetime.date(2024, 6, 15)
    row = {"event_date": d}
    raised = False
    try:
        localize_datetime(row, ["event_date"])
    except AttributeError:
        raised = True

    assert raised, "Expected AttributeError for datetime.date (known bug)"
    print("test_dict_naive_date_raises_attribute_error output is correct")


# -------------------------------------------------------------------
# Dict mode - edge cases
# -------------------------------------------------------------------

def test_dict_none_field_is_skipped():
    """Field with a None value is left untouched."""
    print("=====")
    print("test_dict_none_field_is_skipped")

    row = {"created_at": None, "name": "Alice"}
    result = localize_datetime(row, ["created_at"])

    assert result["created_at"] is None
    assert result["name"] == "Alice"
    print("test_dict_none_field_is_skipped output is correct")


def test_dict_missing_field_is_skipped():
    """Field absent from the dict is silently skipped."""
    print("=====")
    print("test_dict_missing_field_is_skipped")

    row = {"name": "Alice"}
    result = localize_datetime(row, ["created_at"])

    assert "created_at" not in result
    assert result["name"] == "Alice"
    print("test_dict_missing_field_is_skipped output is correct")


def test_dict_nat_raises_custom_validation_error():
    """NaT value in a dict field raises CustomValidationError."""
    print("=====")
    print("test_dict_nat_raises_custom_validation_error")

    row = {"created_at": pd.NaT}
    raised = False
    try:
        localize_datetime(row, ["created_at"])
    except CustomValidationError as e:
        raised = True
        assert "NaT" in e.error

    assert raised, "Expected CustomValidationError was not raised"
    print("test_dict_nat_raises_custom_validation_error output is correct")


def test_dict_multiple_fields():
    """Multiple datetime fields are all localized in a single call."""
    print("=====")
    print("test_dict_multiple_fields")

    row = {
        "created_at": "2024-06-15 10:00:00",
        "updated_at": pd.Timestamp("2024-07-01 12:00:00"),
        "deleted_at": datetime.datetime(2024, 8, 1, 9, 0, 0),
    }
    result = localize_datetime(row, ["created_at", "updated_at", "deleted_at"])

    assert result["created_at"].tzinfo is not None
    assert result["updated_at"].tzinfo is not None
    assert result["deleted_at"].tzinfo is not None
    print("test_dict_multiple_fields output is correct")


def test_dict_non_datetime_fields_untouched():
    """Non-datetime fields in the dict are not modified."""
    print("=====")
    print("test_dict_non_datetime_fields_untouched")

    row = {"created_at": "2024-06-15 10:00:00", "name": "Alice", "age": 30}
    result = localize_datetime(row, ["created_at"])

    assert result["name"] == "Alice"
    assert result["age"] == 30
    print("test_dict_non_datetime_fields_untouched output is correct")
