"""RSS peak checks using memory_profiler (child-process peak MiB).

Workloads live in :mod:`memory_workloads` so the same scenarios can be run from
``scripts/memory_benchmark.py`` for before/after comparisons when you optimize gluestick.
"""

from __future__ import annotations

import pytest

pytest.importorskip("memory_profiler")
from memory_profiler import memory_usage

from tests.function_tests.memory_workloads import (
    COLS_READER_CATALOG_TYPES,
    N_LARGE,
    N_SMALL,
    ROWS_EXPORT_SINGER,
    ROWS_GET_ROW_HASH_ITER,
    ROWS_LAZY_MERGE,
    ROWS_MERGE_ETL,
    ROWS_MERGE_ID,
    ROWS_PL_MERGE,
    ROWS_READER_PARQUET,
    mem_get_row_hash_iterations,
    mem_lazy_read_snapshots,
    mem_lazy_snapshot_records_first_write,
    mem_lazy_snapshot_records_merge,
    mem_merge_id_from_snapshot,
    mem_pl_read_snapshots,
    mem_pl_snapshot_records_first_write,
    mem_pl_snapshot_records_merge,
    mem_reader_get_parquet,
    mem_reader_get_types_from_catalog,
    mem_read_snapshots_etl,
    mem_snapshot_records_merge_etl,
    mem_to_export_pandas_parquet,
    mem_to_export_pandas_singer,
    mem_to_export_polars_csv,
)

# Scaling: only ``read_snapshots`` (pandas) - small vs large peak delta.
MIN_DELTA_PEAK_MIB = 6.0

# Subprocess RSS band (smoke test; not a tight performance budget).
MIN_REPORTED_PEAK_MIB = 35.0
MAX_REASONABLE_PEAK_MIB = 1_500.0


def _peak_mib(func, *args, **kwargs) -> float:
    return float(memory_usage((func, args, kwargs), max_usage=True))


def _assert_peak_sane(peak: float) -> None:
    assert MIN_REPORTED_PEAK_MIB < peak < MAX_REASONABLE_PEAK_MIB, peak


def test_memory_read_snapshots_etl_larger_dataset_higher_peak_rss(tmp_path):
    d1 = tmp_path / "a"
    d2 = tmp_path / "b"
    d1.mkdir()
    d2.mkdir()
    p1 = _peak_mib(mem_read_snapshots_etl, N_SMALL, str(d1))
    p2 = _peak_mib(mem_read_snapshots_etl, N_LARGE, str(d2))
    assert p2 > p1 + MIN_DELTA_PEAK_MIB, f"peak MiB: small={p1} large={p2}"
    _assert_peak_sane(p2)


def test_memory_snapshot_records_merge_etl_peak_rss(tmp_path):
    peak = _peak_mib(mem_snapshot_records_merge_etl, ROWS_MERGE_ETL, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_merge_id_from_snapshot_peak_rss(tmp_path):
    peak = _peak_mib(mem_merge_id_from_snapshot, ROWS_MERGE_ID, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_get_row_hash_peak_rss(tmp_path):
    peak = _peak_mib(mem_get_row_hash_iterations, ROWS_GET_ROW_HASH_ITER, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_reader_get_parquet_peak_rss(tmp_path):
    peak = _peak_mib(mem_reader_get_parquet, ROWS_READER_PARQUET, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_reader_get_types_from_catalog_peak_rss(tmp_path):
    peak = _peak_mib(mem_reader_get_types_from_catalog, COLS_READER_CATALOG_TYPES, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_polars_read_snapshots_peak_rss(tmp_path):
    peak = _peak_mib(mem_pl_read_snapshots, N_LARGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_polars_snapshot_records_first_write_peak_rss(tmp_path):
    peak = _peak_mib(mem_pl_snapshot_records_first_write, N_LARGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_polars_snapshot_records_merge_peak_rss(tmp_path):
    peak = _peak_mib(mem_pl_snapshot_records_merge, ROWS_PL_MERGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_lazy_read_snapshots_peak_rss(tmp_path):
    peak = _peak_mib(mem_lazy_read_snapshots, N_LARGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_lazy_snapshot_records_first_write_peak_rss(tmp_path):
    peak = _peak_mib(mem_lazy_snapshot_records_first_write, N_LARGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_lazy_snapshot_records_merge_peak_rss(tmp_path):
    peak = _peak_mib(mem_lazy_snapshot_records_merge, ROWS_LAZY_MERGE, str(tmp_path), "snap")
    _assert_peak_sane(peak)


def test_memory_to_export_pandas_parquet_peak_rss(tmp_path):
    peak = _peak_mib(mem_to_export_pandas_parquet, N_LARGE, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_to_export_pandas_singer_peak_rss(tmp_path):
    peak = _peak_mib(mem_to_export_pandas_singer, ROWS_EXPORT_SINGER, str(tmp_path))
    _assert_peak_sane(peak)


def test_memory_to_export_polars_csv_peak_rss(tmp_path):
    peak = _peak_mib(mem_to_export_polars_csv, N_LARGE, str(tmp_path))
    _assert_peak_sane(peak)
