from pydantic import ValidationError
from gluestick.date_utils import get_model_datetime_fields
from gluestick.utils.exceptions import CustomValidationError
import traceback
from gluestick.etl_utils import localize_datetime

__all__ = ["validate_model", "handle_validation_error"]

def handle_validation_error(error_message, file_path='error_log.txt', raise_error=False):
        """Handle a validation error by either raising it or logging it to a file.

        When *raise_error* is ``True`` the error is raised immediately as a
        ``CustomValidationError``.  When ``False`` (the default) the error
        message and current traceback are appended to *file_path* instead,
        allowing the caller to continue processing.

        Args:
            error_message (str): The error message to log or raise.
            file_path (str): The path of the file to which the error will be
                logged when *raise_error* is ``False``.
            raise_error (bool): If ``True``, raise a ``CustomValidationError``
                instead of logging.  Defaults to ``False``.

        Raises:
            CustomValidationError: If *raise_error* is ``True``.
        """
        if raise_error:
            raise CustomValidationError(error_message)

        with open(file_path, 'a') as f:
            f.write(f"ERROR: {error_message}\n")
            f.write(f"TRACEBACK:\n{traceback.format_exc()}\n\n")

def validate_model(list, model, config, raise_error=True):
    """Validate and cast a list of dictionaries against a Pydantic model.

    Each dictionary in *list* is localized for datetime fields (using the
    timezone from *config*, defaulting to ``"UTC"``) and then validated by
    instantiating the given Pydantic *model*. Records that pass validation
    are collected and returned; records that fail are logged (or raise an
    error, depending on *raise_error*).

    Args:
        list (list[dict]): Raw records to validate.
        model: A Pydantic model **class** used for validation and casting.
            Must expose ``model_fields`` and ``schema_name``.
        config (dict): Configuration dictionary. The ``"timezone"`` key is
            used for datetime localization (defaults to ``"UTC"``).
        raise_error (bool): If ``True`` (default), validation or datetime
            errors are raised immediately as ``CustomValidationError``.
            If ``False``, errors are written to the error log file and
            the record is skipped.

    Returns:
        list: A list of validated Pydantic model instances for every record
        that passed validation.

    Raises:
        CustomValidationError: If *raise_error* is ``True`` and a record
            fails Pydantic validation or contains invalid datetime values.
    """
    output_list = []
    datetime_fields = get_model_datetime_fields(model)
    for value in list:
        try:
            timezone = config.get("timezone", "UTC")
            value = localize_datetime(value, datetime_fields, timezone)
            validated_value = model(**value)
            output_list.append(validated_value)
        except ValidationError as ve:
            for error in ve.errors():
                field_name = error["loc"][0]
                expected_type = error["type"]
                invalid_value = error["input"]
                error_message = f"Field '{field_name}' in model {model.schema_name} failed to be casted as '{expected_type}', value trying to be casted: '{invalid_value}'"
                handle_validation_error(error_message, raise_error=raise_error)
            continue
        except CustomValidationError as e:
            handle_validation_error(e.error, raise_error=raise_error)
    return output_list
