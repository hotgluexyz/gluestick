"""Run post-process plugins before export."""

import importlib.util
import json
import logging
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from pydantic import Field, create_model

logger = logging.getLogger(__name__)


def _annotation_for_series(series: pd.Series):
    if pd.api.types.is_bool_dtype(series):
        return bool
    if pd.api.types.is_integer_dtype(series):
        return int
    if pd.api.types.is_float_dtype(series):
        return float
    if pd.api.types.is_datetime64_any_dtype(series):
        return datetime
    return str


def _extend_model_with_new_columns(model, df: pd.DataFrame, input_columns: set):
    """Add dataframe columns a custom plugin introduced onto the export model."""
    if model is None or not hasattr(model, "model_fields"):
        return model

    existing_fields = set(model.model_fields)
    new_columns = [
        column
        for column in df.columns
        if column not in input_columns and column not in existing_fields
    ]
    if not new_columns:
        return model

    field_definitions = {
        column: (Optional[_annotation_for_series(df[column])], Field(default=None))
        for column in new_columns
    }
    return create_model(f"{model.__name__}Custom", __base__=model, **field_definitions)


def _load_tenant_config(snapshot_dir: str) -> dict:
    config_path = os.path.join(snapshot_dir, "tenant-config.json")
    if not os.path.isfile(config_path):
        return {}
    with open(config_path) as config_file:
        return json.load(config_file)


def execute_custom_plugins(
    stream_name, data_df, model, key_properties, output_dir
) -> tuple[pd.DataFrame, Any, Any]:
    """Run ``{ROOT_DIR}/plugins/{stream_name}_post_process.py`` before export.

    A missing plugins folder, or no file for this stream, leaves the dataframe,
    model, and keys unchanged. The module must expose
    ``main(context, df, model, key_properties, tenant_config)`` and return
    ``(df, model, key_properties)``. Columns the plugin adds are added to the
    export model here. A plugin that fails to load, has no ``main``, returns
    the wrong shape, or raises stops the export.
    """
    root_dir = os.environ.get("ROOT_DIR", ".")
    plugins_dir = os.path.join(root_dir, "plugins")
    if not os.path.isdir(plugins_dir):
        return data_df, model, key_properties

    suffix = "_post_process.py"
    plugin_files = sorted(
        os.path.join(plugins_dir, name)
        for name in os.listdir(plugins_dir)
        if name.endswith(suffix) and name[: -len(suffix)] == stream_name
    )
    if not plugin_files:
        return data_df, model, key_properties

    snapshot_dir = os.path.join(root_dir, "snapshots")
    context = {
        "ROOT_DIR": root_dir,
        "INPUT_DIR": os.path.join(root_dir, "sync-output"),
        "OUTPUT_DIR": output_dir or os.path.join(root_dir, "etl-output"),
        "SNAPSHOT_DIR": snapshot_dir,
        "tenant_id": os.environ.get("USER_ID", os.environ.get("TENANT")),
        "flow_id": os.environ.get("FLOW"),
    }
    tenant_config = _load_tenant_config(snapshot_dir)

    current_df, current_model, current_keys = data_df, model, key_properties
    for plugin_file in plugin_files:
        module_name = (
            f"custom_plugin_{stream_name}_"
            f"{os.path.splitext(os.path.basename(plugin_file))[0]}"
        )
        spec = importlib.util.spec_from_file_location(module_name, plugin_file)
        if spec is None or spec.loader is None:
            raise ImportError(
                f"Could not load custom plugin {plugin_file} for {stream_name}"
            )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        main = getattr(module, "main", None)
        if not callable(main):
            raise TypeError(
                f"Custom plugin {plugin_file} has no main for {stream_name}"
            )

        input_columns = set(current_df.columns)
        plugin_df = current_df.copy(deep=False)
        logger.info("Running custom plugin %s for %s", plugin_file, stream_name)
        result = main(context, plugin_df, current_model, current_keys, tenant_config)
        if not isinstance(result, tuple) or len(result) != 3:
            raise ValueError(
                f"Custom plugin {plugin_file} main must return (df, model, key_properties)"
            )
        current_df, current_model, current_keys = result
        current_model = _extend_model_with_new_columns(
            current_model, current_df, input_columns
        )

    return current_df, current_model, current_keys
