import logging
import os
import json
import datetime
import pandas as pd
import re
import copy
from pydantic import ValidationError
from contextlib import suppress
import traceback

__all__ = ["CustomValidationError", "Utils"]

logger = logging.getLogger('Utils')

logger.info('Pandas Version: ' + pd.__version__)

class CustomValidationError(Exception):
    def __init__(self, error):
        super().__init__(error)
        self.error = error

class Utils:

    @staticmethod
    def establish_directories(global_vars):

        def get_var(var_name, default_value):
            return os.getenv(var_name, global_vars.get(var_name, default_value))

        ROOT_DIR = get_var("ROOT_DIR", ".")
        parameters = {
            "ROOT_DIR": ROOT_DIR,
            "base_input_dir": get_var("base_input_dir", f"{ROOT_DIR}/sync-output"),
            "output_dir": get_var("output_dir", f"{ROOT_DIR}/etl-output"),
            "snapshot_dir": get_var("snapshot_dir", f"{ROOT_DIR}/snapshots"),
            "tmp_dir": get_var("tmp_dir", f"{ROOT_DIR}/tmp"),
            "config_json": get_var("config_json", f"{ROOT_DIR}/config.json"),
            "today": get_var("today", None),
            "tenant_id": get_var("USER_ID", get_var("TENANT", None)),
            "flow_id": get_var("FLOW", None),
        }

        if parameters["today"] is None:
            parameters["today"] = datetime.date.today()
        else:
            parameters["today"] = datetime.datetime.strptime(parameters["today"], '%Y%m%d')

        logger.info(json.dumps(parameters, indent=4, sort_keys=True, default=str))

        with suppress(FileExistsError):
            os.makedirs(parameters["base_input_dir"])
        with suppress(FileExistsError):
            os.makedirs(parameters["output_dir"])
        with suppress(FileExistsError):
            os.makedirs(parameters["snapshot_dir"])
        with suppress(FileExistsError):
            os.makedirs(parameters["tmp_dir"])

        config_json = parameters["config_json"]

        if not os.path.exists(config_json):
            config_json = None

        parameters['config_json'] = config_json

        to_return = [
            parameters["ROOT_DIR"],
            parameters["base_input_dir"],
            parameters["output_dir"],
            parameters["snapshot_dir"],
            parameters["tenant_id"],
            parameters["flow_id"],
            parameters["today"],
            parameters["tmp_dir"],
            parameters["config_json"],
        ]

        return tuple(to_return)

    @staticmethod
    def load_config_json_data(config_json_data, config_vars):

        if config_json_data is None:
            return tuple(config_vars.values())

        for variable in config_vars.keys():
            if variable not in config_json_data or config_json_data[variable] == "":
                config_json_data[variable] = config_vars[variable]

        return config_json_data

    @staticmethod
    def load_config_json(config_json, config_vars):

        if not config_json or not os.path.exists(config_json):
            return config_vars

        logger.info(f"Loading config.json file from {config_json}.")
        with open(config_json) as f:
            config_json_data = json.load(f)

        return Utils.load_config_json_data(config_json_data, config_vars)

    @staticmethod
    def pick(objects, id_field, filter_ids, target_fields, partition_key=None, partition_key_value=None):
        """
        Extracts specific attributes from objects in a list based on filter criteria.

        Parameters:
            objects (list or DataFrame): A list or dataframe of dictionaries or objects containing attributes.
            id_field (str): The attribute name representing a unique identifier for each object.
            filter_ids (str, int, list, or set): A single ID (str or int) or a list/set of IDs (str or int) to filter objects.
            target_fields (str or list): The attribute name or a list of attribute names to extract
                                        from the selected objects.
            partition_key (str or None): Optional key to further filter objects when using a DataFrame.
                                         If provided, only rows where this column equals partition_key_value will be included.
            partition_key_value: Value to match against the partition_key column. Only used when partition_key is provided.

        Returns:
            list, dict, or str:
                - If `filter_ids` is a single ID (string):
                    - Returns a single value (if `target_fields` is a string).
                    - Returns a single dictionary (if `target_fields` is a list of strings).
                - If `filter_ids` is a list or set:
                    - Returns a list of values (if `target_fields` is a string).
                    - Returns a list of dictionaries (if `target_fields` is a list of strings).

        Raises:
            ValueError: If `target_fields` is neither a string nor a list of strings.

        Example:
            input_objects = [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Charlie", "age": 35},
            ]

            # Single ID, single field
            result = pick(input_objects, "id", 1, "name")
            # Output: "Alice"

            # Single ID, multiple fields
            result = pick(input_objects, "id", 1, ["name", "age"])
            # Output: {"name": "Alice", "age": 30}

            # Multiple IDs, single field
            result = pick(input_objects, "id", [1, 3], "name")
            # Output: ["Alice", "Charlie"]

            # Multiple IDs, multiple fields
            result = pick(input_objects, "id", [1, 3], ["name", "age"])
            # Output: [{"name": "Alice", "age": 30}, {"name": "Charlie", "age": 35}]
            
            # With partition filtering (using pandas DataFrame)
            df = pd.DataFrame([
                {"id": 1, "name": "Alice", "age": 30, "dept": "HR"},
                {"id": 2, "name": "Bob", "age": 25, "dept": "IT"},
                {"id": 3, "name": "Charlie", "age": 35, "dept": "HR"}
            ])
            result = pick(df, "id", [1, 3], ["name", "age"], "dept", "HR")
            # Output: [{"name": "Alice", "age": 30}, {"name": "Charlie", "age": 35}]
        """
        # Ensure filter_ids is a set for easier filtering
        if isinstance(filter_ids, (int, str)):
            filter_ids = {filter_ids}
            is_single = True
        elif isinstance(filter_ids, (list, set)):
            filter_ids = set(filter_ids)
            is_single = False
        elif pd.isna(filter_ids):
            return None
        else:
            raise ValueError("filter_ids must be a integer, string, list, or set.")

        # Helper to extract attributes from an object
        def extract(obj):
            if isinstance(target_fields, str):
                return obj[target_fields]
            elif isinstance(target_fields, list):
                return {field: obj[field] for field in target_fields}
            else:
                raise ValueError("target_fields must be a string or a list of strings.")

        # Filter objects based on filter_ids and extract attributes
        if objects is None:
            return None
        if isinstance(objects, list):
            # TODO: add support for partition_key and partition_key_value if needed
            results = [extract(obj) for obj in objects if obj[id_field] in filter_ids]
        elif isinstance(objects, pd.DataFrame):
            # Process pandas DataFrame
            if partition_key:
                filtered_objects = objects[objects[id_field].isin(filter_ids) & (objects[partition_key] == partition_key_value)]
            else:
                filtered_objects = objects[objects[id_field].isin(filter_ids)]
            # Convert rows to dictionaries and process them
            results = [extract(row.to_dict()) for _, row in filtered_objects.iterrows()]

        # Return a single result if filter_ids was a string
        if is_single and len(results) == 1:
            return results[0] if results else None
        if not len(results):
            return None
        return results

    @staticmethod
    def process_custom_fields(row):
        """
        Process a dictionary into a list of dictionaries with custom field structure: 'name' and string 'value' keys.

        Args:
            data (dict | pd.Series): Input data, either a dictionary or a list of dictionaries.

        Returns:
            list[dict]: A list of dictionaries in the format {"name": key, "value": value}.
        """
        # Convert a single dictionary to the desired structure
        if isinstance(row, dict):
            # Handle dictionary input
            return [{"name": key, "value": value} for key, value in row.items() if not pd.isna(value)]
        elif isinstance(row, pd.Series):
            # Handle DataFrame row (pd.Series) input
            return [{"name": key, "value": value} for key, value in row.to_dict().items() if not pd.isna(value)]

    @staticmethod
    def pick_regex(row, regex_field, return_as_cf=False):
        """
        Filters a dictionary, keeping only the key-value pairs where the key matches the regex.

        Args:
            data (dict): Input dictionary to filter.
            regex (str): Regular expression to match keys.

        Returns:
            dict: A filtered dictionary with matching key-value pairs.
        """

        pattern = re.compile(regex_field)
        result = {key: value for key, value in row.items() if pattern.match(key)}
        if return_as_cf:
            result = Utils.process_custom_fields(result)
        return result
    
    @staticmethod
    def map_fields(row, mapping, other_data):
        """Maps the row values according to the mapping dict.

        Notes
        -----
        This function will iterate through all the values of a mapping dict
        and map the values from the row accordingly

        Parameters
        ----------
        row: dict or dataframe row with the values to be mapped
        mapping: dict that estabilsh how to map the fields
        other_data: dict containing additional data sources that can be referenced in the mapping

        Returns
        -------
        return: dict
            dict with the mapped data.

        """
        output = {}
        for key, value in mapping.items():
            if isinstance(value, list):
                out_list = []
                for v in value:
                    mapped = Utils.map_fields(row, v, other_data)
                    if mapped:
                        out_list.append(mapped)
                if out_list:
                    output[key] = out_list
            elif isinstance(value, dict):
                if "pick" in value:
                    kwargs = copy.deepcopy(value["pick"])
                    if kwargs.get("filter_ids").startswith("rec."):
                        field = kwargs["filter_ids"].replace("rec.", "")  # Extract the field part after 'rec.'
                        filter_id = row.get(field)
                        if not filter_id:
                            output[key] = None
                            continue
                        kwargs["filter_ids"] = row.get(field)
                    # read data
                    
                    if kwargs.get("partition_key"):
                        kwargs["partition_key_value"] = row.get(kwargs["partition_key"])
                    else:
                        kwargs["partition_key"] = None
                        kwargs["partition_key_value"] = None
                    object_name = kwargs.pop("objects", None)
                    kwargs["objects"] = other_data.get(object_name)
                    if kwargs["objects"] is not None and not kwargs["objects"].empty:
                        output[key] = Utils.pick(**kwargs)
                    continue
                if "pickregex" in value:
                    kwargs = value["pickregex"]
                    output[key] = Utils.pick_regex(row, **kwargs)
                    continue
                mapped = Utils.map_fields(row, value, other_data)
                if mapped:
                    output[key] = mapped
            elif value is not None:
                if isinstance(row.get(value), list) or not pd.isna(row.get(value)):
                    output[key] = row.get(value)
        return output
    
    @staticmethod
    def map_fields_df(df, mapping, other_data):
        """Maps the DataFrame values according to the mapping dict, optimized for large datasets.

        Parameters
        ----------
        df: pandas DataFrame with the values to be mapped
        mapping: dict that establishes how to map the fields
        other_data: dict containing additional data sources that can be referenced in the mapping

        Returns
        -------
        DataFrame: DataFrame with the mapped data
        """
        # For simple scalar mappings, we can do this efficiently
        simple_mappings = {}
        complex_mappings = {}
        
        # Split mappings into simple and complex
        for key, value in mapping.items():
            if isinstance(value, str) or pd.isna(value):
                simple_mappings[key] = value
            else:
                complex_mappings[key] = value

        for new_col, source_col in simple_mappings.items():
            if source_col in df.columns:
                df[new_col] = df[source_col].astype(object).where(pd.notna(df[source_col]), None)
        
        # Process complex mappings if any exist
        if complex_mappings:
            for key, value in complex_mappings.items():
                if isinstance(value, dict) and "pick" in value:
                    pick_args = copy.deepcopy(value["pick"])
                    objects_df = other_data.get(pick_args["objects"])
                    if objects_df is None or objects_df.empty:
                        continue
                    
                    filter_ids = pick_args["filter_ids"]
                    id_field = pick_args["id_field"]
                    partition_key = pick_args.get("partition_key")
                    
                    if filter_ids.startswith("rec."):
                        # filter_ids = filter_ids.split(".")[1]
                        filter_ids = filter_ids.replace("rec.", "")  # Extract the field part after 'rec.'
                    
                    target_fields = pick_args["target_fields"]

                    if pick_args.get("is_list", False):
                        # Handle list of IDs to list of objects
                        if partition_key:
                            df[key] = df.apply(
                                lambda row: Utils.pick(objects_df, id_field, row[filter_ids], target_fields, partition_key, row[partition_key]),
                                axis=1
                            )
                        else:
                            df[key] = df.apply(
                                lambda row: Utils.pick(objects_df, id_field, row[filter_ids], target_fields),
                                axis=1
                            )
                        continue

                    if partition_key is not None:
                        objects_df = objects_df[[id_field, pick_args["target_fields"], partition_key]].drop_duplicates(subset=[id_field, partition_key], keep='last')
                        df = df.merge(
                            objects_df.rename(columns={pick_args["target_fields"]: key}),
                            left_on=[filter_ids, partition_key],
                            right_on=[id_field, partition_key],
                            how="left",
                            suffixes=('', '_y')  # Keep left (original) column names unchanged
                        )
                    else:
                        objects_df = objects_df[[id_field, pick_args["target_fields"]]].drop_duplicates(subset=[id_field], keep='last')

                        df = df.merge(
                            objects_df.rename(columns={pick_args["target_fields"]: key}),
                            left_on=filter_ids,
                            right_on=id_field,
                            how="left",
                            suffixes=('', '_y')  # Keep left (original) column names unchanged
                        )
                        
                        # Drop the id_field column from objects_df that was added during merge
                        # Check for both possible column names since pandas behavior can vary
                        if f'{id_field}_y' not in df.columns and id_field in df.columns and filter_ids != id_field:
                            # If the id_field was added and it's different from the filter_ids column
                            df = df.drop(id_field, axis=1)
                        
                    df[key] = df[key].astype(object).where(pd.notna(df[key]), None)

                    # Drop the right-side ID column that was added during merge
                    if f'{id_field}_y' in df.columns:
                        df = df.drop(f'{id_field}_y', axis=1)
                    
                    # Drop the right-side partition_key column if it was added during merge
                    if f'{partition_key}_y' in df.columns:
                        df = df.drop(f'{partition_key}_y', axis=1)
                        
        return df
    
    @staticmethod
    def log_error_to_file(error_message, file_path='error_log.txt'):
        """
        Logs the error message to a file with a timestamp.

        Args:
            error_message (str): The error message to log.
            file_path (str): The path of the file to which the error will be logged.
        """
        with open(file_path, 'a') as f:
            f.write(f"ERROR: {error_message}\n")
            f.write(f"TRACEBACK:\n{traceback.format_exc()}\n\n")
    
    @staticmethod
    def get_model_datetime_fields(model):
         # Get datetime fields from the Vendor model
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

    @staticmethod
    def localize_datetimes(row, datetime_fields, timezone):                
        for field in datetime_fields:
            if row.get(field):
                value = row[field]
                # Check for NaT values, which are neither None nor valid timestamps
                if value is not None and pd.isna(value):
                    error_message = f"Field '{field}' contains a NaT (Not a Time) value which is not allowed against the record: {row}"
                    raise CustomValidationError(error_message)
                if isinstance(value, str):
                    value = pd.to_datetime(value)
                    if value.tzinfo is None:
                        value = value.tz_localize(timezone)
                    else:
                        value = value.tz_convert(timezone)
                    row[field] = value
                elif isinstance(value, pd.Timestamp):
                    row[field] = value.tz_localize(timezone) if value.tzinfo is None else value
                elif isinstance(value, datetime.datetime):
                    row[field] = value.replace(tzinfo=datetime.timezone.utc) if value.tzinfo is None else value
                elif isinstance(value, datetime.date):
                    row[field] = value.replace(tzinfo=datetime.timezone.utc) if value.tzinfo is None else value
        return row
    
    @staticmethod
    def validate_model(list, model, config):
        output_list = []
        datetime_fields = Utils.get_model_datetime_fields(model)
        for value in list:
            try:
                # localize datetime fields
                timezone = config.get("timezone", "UTC")
                value = Utils.localize_datetimes(value, datetime_fields, timezone)
                # Attempt to cast to Vendor model
                validated_value = model(**value)
                output_list.append(validated_value)
            except ValidationError as ve:
                # Catch validation errors and log the failed fields
                for error in ve.errors():
                    field_name = error['loc'][0]
                    expected_type = error['type']
                    invalid_value = error['input']
                    error_message = f"Field '{field_name}' in model {model.schema_name} failed to be casted as '{expected_type}', value trying to be casted: '{invalid_value}'"
                    Utils.log_error_to_file(error_message)
                continue 
            except CustomValidationError as e:
                Utils.log_error_to_file(e.error)
        return output_list
    
    @staticmethod
    def should_map_table(model_name, config):
        """
        Checks if a table is selected for mapping.

        Args:
            model_name (str): The name of the model/table to check.
            config (dict): Configuration containing the 'selected_tables' mapping.

        Returns:
            bool: True if the table is selected, False otherwise.
        """
        should_map = config.get("selected_tables", {}).get(model_name)
        if not should_map:
            print(f"Skipping mapping for {model_name}, not selected.")
        return bool(should_map)
    
    @staticmethod
    def read_data(stream, reader):
        data = reader.get(stream, catalog_types=True)
        if data is not None:
            data = data.replace({pd.NA: None})
        return data
