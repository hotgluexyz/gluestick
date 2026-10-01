"""Tests for custom post-process plugins in `to_export`."""

import json
from pathlib import Path

import pandas as pd
import pytest
from pydantic import BaseModel

from gluestick.etl_utils import to_export


def _pd_small():
    return pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})


class _Order(BaseModel):
    id: int
    name: str


def test_pandas_custom_plugin_extends_export(monkeypatch, tmp_path):
    root = tmp_path / "job"
    plugins = root / "plugins"
    plugins.mkdir(parents=True)
    (root / "snapshots").mkdir()
    (root / "snapshots" / "tenant-config.json").write_text(json.dumps({"flag": True}))
    (plugins / "orders_post_process.py").write_text(
        "def main(context, df, model, key_properties, tenant_config):\n"
        "    assert context['ROOT_DIR']\n"
        "    assert tenant_config['flag'] is True\n"
        "    df = df.copy()\n"
        "    df['extra'] = 'x'\n"
        "    return df, model, ['id', 'extra']\n"
    )
    monkeypatch.setenv("ROOT_DIR", str(root))

    to_export(
        _pd_small(),
        name="orders",
        output_dir=str(tmp_path),
        keys=["id"],
        export_format="singer",
        unified_model=_Order,
    )

    lines = [
        json.loads(line)
        for line in (tmp_path / "data.singer").read_text().splitlines()
        if line.strip()
    ]
    schema = next(line for line in lines if line["type"] == "SCHEMA")
    assert "extra" in schema["schema"]["properties"]
    assert schema["key_properties"] == ["id", "extra"]
    record = next(line for line in lines if line["type"] == "RECORD")
    assert record["record"]["extra"] == "x"


@pytest.mark.parametrize(
    "body",
    [
        "    return df\n",
        "    return None, model, key_properties\n",
    ],
)
def test_pandas_custom_plugin_bad_return_stops_export(monkeypatch, tmp_path, body):
    root = tmp_path / "job"
    plugins = root / "plugins"
    plugins.mkdir(parents=True)
    (plugins / "orders_post_process.py").write_text(
        "def main(context, df, model, key_properties, tenant_config):\n" + body
    )
    monkeypatch.setenv("ROOT_DIR", str(root))

    with pytest.raises(ValueError, match="must return"):
        to_export(
            _pd_small(),
            name="orders",
            output_dir=str(tmp_path),
            keys=["id"],
            export_format="csv",
        )
