"""Tests for `gluestick.etl_utils.to_export` (pandas + Polars dispatch)."""

import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gluestick.etl_utils import to_export


def _pd_small():
    return pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})


def _pl_small_df():
    return pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})


def _pl_small_lf():
    return pl.LazyFrame({"id": [1, 2], "name": ["a", "b"]})


def test_to_export_unregistered_type_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="not implemented"):
        to_export([1, 2, 3], name="x", output_dir=".")


def test_pandas_singer_creates_data_singer(tmp_path):
    df = _pd_small()
    out = Path(tmp_path) / "data.singer"
    to_export(
        df,
        name="orders",
        output_dir=str(tmp_path),
        keys=["id"],
        export_format="singer",
    )
    assert out.is_file()
    lines = [json.loads(line) for line in out.read_text().splitlines() if line.strip()]
    types = {x["type"] for x in lines}
    assert "SCHEMA" in types and "RECORD" in types and "STATE" in types


@pytest.mark.parametrize(
    "export_format,filename",
    [
        ("csv", "stream.csv"),
        ("parquet", "stream.parquet"),
        ("json", "stream.json"),
        ("jsonl", "stream.jsonl"),
    ],
)
def test_pandas_export_formats_write_file(tmp_path, export_format, filename):
    df = _pd_small()
    to_export(
        df,
        name="stream",
        output_dir=str(tmp_path),
        keys=["id"],
        export_format=export_format,
    )
    path = Path(tmp_path) / filename
    assert path.is_file()
    if export_format == "csv":
        assert len(pd.read_csv(path)) == 2
    elif export_format == "parquet":
        assert len(pd.read_parquet(path)) == 2
    elif export_format == "json":
        assert len(json.loads(path.read_text())) == 2
    else:
        assert len([ln for ln in path.read_text().splitlines() if ln.strip()]) == 2


def test_pandas_unknown_export_format_falls_through_to_csv(tmp_path):
    """Non-(singer|parquet|json|jsonl) uses the final `to_csv` branch."""
    df = _pd_small()
    to_export(
        df,
        name="misc",
        output_dir=str(tmp_path),
        keys=["id"],
        export_format="not_a_real_format",
    )
    path = Path(tmp_path) / "misc.csv"
    assert path.is_file()
    assert len(pd.read_csv(path)) == 2


def test_pandas_output_file_prefix_composes_filename(tmp_path):
    df = _pd_small()
    to_export(
        df,
        name="t",
        output_dir=str(tmp_path),
        export_format="csv",
        output_file_prefix="run_",
        keys=["id"],
    )
    assert (Path(tmp_path) / "run_t.csv").is_file()


def test_pandas_hg_unified_output_env_overrides_stream_name(monkeypatch, tmp_path):
    monkeypatch.setenv("HG_UNIFIED_OUTPUT_MYSTREAM", "renamed")
    df = _pd_small()
    to_export(
        df,
        name="mystream",
        output_dir=str(tmp_path),
        export_format="csv",
        keys=["id"],
    )
    assert (Path(tmp_path) / "renamed.csv").is_file()
    assert not (Path(tmp_path) / "mystream.csv").is_file()


def test_polars_dataframe_parquet_csv_json_jsonl_singer(tmp_path):
    df = _pl_small_df()
    base = Path(tmp_path)

    to_export(df, name="p", output_dir=str(tmp_path), export_format="parquet", keys=["id"])
    assert pl.read_parquet(base / "p.parquet").height == 2

    to_export(df, name="c", output_dir=str(tmp_path), export_format="csv", keys=["id"])
    assert pl.read_csv(base / "c.csv").height == 2

    to_export(df, name="j", output_dir=str(tmp_path), export_format="json", keys=["id"])
    assert (base / "j.json").is_file()

    to_export(df, name="jl", output_dir=str(tmp_path), export_format="jsonl", keys=["id"])
    assert (base / "jl.jsonl").is_file()

    to_export(df, name="s", output_dir=str(tmp_path), export_format="singer", keys=["id"])
    singer_path = base / "data.singer"
    assert singer_path.is_file()


def test_polars_dataframe_unsupported_format_raises(tmp_path):
    df = _pl_small_df()
    with pytest.raises(ValueError, match="Unsupported export format"):
        to_export(
            df,
            name="x",
            output_dir=str(tmp_path),
            export_format="not_a_real_format",
            keys=["id"],
        )


def test_polars_lazyframe_parquet_csv_singer(tmp_path):
    lf = _pl_small_lf()
    base = Path(tmp_path)

    to_export(lf, name="p", output_dir=str(tmp_path), export_format="parquet", keys=["id"])
    assert pl.read_parquet(base / "p.parquet").height == 2

    to_export(lf, name="c", output_dir=str(tmp_path), export_format="csv", keys=["id"])
    assert pl.read_csv(base / "c.csv").height == 2

    to_export(lf, name="s", output_dir=str(tmp_path), export_format="singer", keys=["id"])
    assert (base / "data.singer").is_file()


@pytest.mark.parametrize("bad_format", ["json", "jsonl"])
def test_polars_lazyframe_unsupported_format_raises(tmp_path, bad_format):
    lf = _pl_small_lf()
    with pytest.raises(ValueError, match="Unsupported export format"):
        to_export(
            lf,
            name="x",
            output_dir=str(tmp_path),
            export_format=bad_format,
            keys=["id"],
        )


def test_polars_lazyframe_output_file_prefix(tmp_path):
    lf = _pl_small_lf()
    to_export(
        lf,
        name="t",
        output_dir=str(tmp_path),
        export_format="csv",
        output_file_prefix="pre_",
        keys=["id"],
    )
    assert (Path(tmp_path) / "pre_t.csv").is_file()


def test_polars_dataframe_output_file_prefix(tmp_path):
    df = _pl_small_df()
    to_export(
        df,
        name="t",
        output_dir=str(tmp_path),
        export_format="csv",
        output_file_prefix="z_",
        keys=["id"],
    )
    assert (Path(tmp_path) / "z_t.csv").is_file()


def test_pandas_singer_export_10k_records(tmp_path):
    n = 10_000
    df = pd.DataFrame(
        {
            "id": range(n),
            "name": [f"row_{i}" for i in range(n)],
        }
    )
    to_export(
        df,
        name="bulk",
        output_dir=str(tmp_path),
        export_format="singer",
        keys=["id"],
    )
    out = Path(tmp_path) / "data.singer"
    assert out.is_file()
    lines = [json.loads(line) for line in out.read_text().splitlines() if line.strip()]
    kinds = [x["type"] for x in lines]
    assert kinds[0] == "SCHEMA"
    assert kinds[-1] == "STATE"
    records = [x for x in lines if x["type"] == "RECORD"]
    assert len(records) == n
    assert records[0]["stream"] == "bulk"
    r0 = records[0]["record"]
    assert r0["id"] == 0 and r0["name"] == "row_0"
    assert records[42]["record"]["name"] == "row_42"
    assert records[-1]["record"]["id"] == n - 1
