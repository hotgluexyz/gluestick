import os
import json
import logging
import datetime
from contextlib import suppress

logger = logging.getLogger("ConfigUtils")

__all__ = ["establish_directories", "load_config_json"]


def establish_directories(global_vars):
    """Resolve directory paths, create them on disk, and return key parameters.

    Each parameter is resolved by first checking the corresponding
    **environment variable**, then falling back to *global_vars*, and finally
    using a built-in default relative to ``ROOT_DIR``.

    The following directories are created if they do not already exist:

    - ``base_input_dir`` - sync output (default ``<ROOT_DIR>/sync-output``)
    - ``output_dir`` - ETL output (default ``<ROOT_DIR>/etl-output``)
    - ``snapshot_dir`` - snapshots (default ``<ROOT_DIR>/snapshots``)
    - ``tmp_dir`` - temporary files (default ``<ROOT_DIR>/tmp``)

    The ``today`` parameter is parsed from the ``%Y%m%d`` format if provided
    as a string, otherwise it defaults to ``datetime.date.today()``.
    The ``config_json`` path is set to ``None`` if the file does not exist.

    Args:
        global_vars (dict): Fallback variable mapping consulted when the
            corresponding environment variable is not set. Recognised keys
            include ``ROOT_DIR``, ``base_input_dir``, ``output_dir``,
            ``snapshot_dir``, ``tmp_dir``, ``config_json``, ``today``,
            ``USER_ID`` / ``TENANT``, and ``FLOW``.

    Returns:
        tuple: A 9-element tuple containing:
            ``(ROOT_DIR, base_input_dir, output_dir, snapshot_dir,
            tenant_id, flow_id, today, tmp_dir, config_json)``.
    """

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
        parameters["today"] = datetime.datetime.strptime(parameters["today"], "%Y%m%d")

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

    parameters["config_json"] = config_json

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


def _load_config_json_data(config_json_data, config_vars):

    if config_json_data is None:
        return tuple(config_vars.values())

    for variable in config_vars.keys():
        if variable not in config_json_data or config_json_data[variable] == "":
            config_json_data[variable] = config_vars[variable]

    return config_json_data


def load_config_json(config_json, config_vars):
    """Load configuration from a JSON file and merge it with default variables.

    Reads a JSON configuration file from the given path and merges its
    contents with the provided default configuration variables. Any key
    present in ``config_vars`` that is missing or has an empty-string value
    in the JSON file will retain its default value from ``config_vars``.

    If the file path is falsy (``None`` / empty string) or the file does
    not exist on disk, the original ``config_vars`` dictionary is returned
    unchanged.

    Parameters
    ----------
    config_json : str or None
        File-system path to a JSON configuration file.  Typically
        resolved by :meth:`establish_directories` (e.g.
        ``<ROOT_DIR>/config.json``).
    config_vars : dict
        Dictionary of default configuration variables.  Keys that
        also appear in the JSON file will be overwritten by the
        file values; keys absent from the file are preserved.

    Returns
    -------
    dict
        A merged configuration dictionary containing all keys from
        ``config_vars`` with values overridden by the JSON file
        where present and non-empty.

    See Also
    --------
    load_config_json_data : Performs the in-memory merge of parsed
        JSON data with default variables.
    establish_directories : Resolves the ``config_json`` path from
        environment variables or ``global_vars``.

    Examples
    --------
    >>> defaults = {"batch_size": 100, "timeout": 30}
    >>> # config.json contains: {"batch_size": 500}
    >>> Utils.load_config_json("/path/to/config.json", defaults)
    {'batch_size': 500, 'timeout': 30}

    >>> # When file does not exist, defaults are returned as-is
    >>> Utils.load_config_json(None, defaults)
    {'batch_size': 100, 'timeout': 30}
    """
    if not config_json or not os.path.exists(config_json):
        return config_vars

    logger.info(f"Loading config.json file from {config_json}.")
    with open(config_json) as f:
        config_json_data = json.load(f)

    return _load_config_json_data(config_json_data, config_vars)
