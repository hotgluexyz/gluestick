#!/usr/bin/env python3
"""Print peak RSS (MiB) per scenario using the same workloads as ``test_memory_usage``.

Usage (from repo root, with dev deps)::

    pip install ".[test]"
    python scripts/memory_benchmark.py
    python scripts/memory_benchmark.py --json > before.json

After changing gluestick code, run again and diff::

    python scripts/memory_benchmark.py --json > after.json
    python -c "import json; b=json.load(open('before.json')); a=json.load(open('after.json')); [print(k, b[k], '->', a[k], '('+str(round(100*(a[k]-b[k])/b[k],2))+'%)') for k in b]"

Or use ``jq``/diff tools on the JSON objects.

This measures **child-process** peak RSS (same as memory_profiler in tests), so it is
useful for relative before/after checks on the **same machine**, not for absolute MiB.

While measuring, this script redirects OS stdout/stderr to ``/dev/null`` so the
child process spawned by ``memory_profiler`` does not mix gluestick ``print`` output
with ``--json`` on your terminal or in a redirected file. Library code is unchanged.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from memory_profiler import memory_usage

# Repo root must be on path for `tests.function_tests.memory_workloads` when run as
# ``python scripts/memory_benchmark.py`` (see PEP 8 E402 for the following import).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.function_tests.memory_workloads import (  # noqa: E402
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


@contextlib.contextmanager
def _silence_process_stdio():
    """Point fd 1 and 2 at /dev/null so spawned workers do not print to the terminal."""
    devnull = os.open(os.devnull, os.O_WRONLY)
    saved_out = os.dup(1)
    saved_err = os.dup(2)
    try:
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        yield
    finally:
        os.dup2(saved_out, 1)
        os.dup2(saved_err, 2)
        os.close(saved_out)
        os.close(saved_err)
        os.close(devnull)


def _peak_mib(func, *args, **kwargs) -> float:
    with _silence_process_stdio():
        return float(memory_usage((func, args, kwargs), max_usage=True))


def _run_scenarios(root: Path) -> dict[str, float]:
    """Return scenario name -> peak MiB."""
    out: dict[str, float] = {}

    a, b = root / "scale_a", root / "scale_b"
    a.mkdir()
    b.mkdir()
    out["read_snapshots_etl_N_SMALL"] = _peak_mib(mem_read_snapshots_etl, N_SMALL, str(a))
    out["read_snapshots_etl_N_LARGE"] = _peak_mib(mem_read_snapshots_etl, N_LARGE, str(b))

    d = root / "merge_etl"
    d.mkdir()
    out["snapshot_records_merge_etl"] = _peak_mib(
        mem_snapshot_records_merge_etl, ROWS_MERGE_ETL, str(d)
    )

    d = root / "merge_id"
    d.mkdir()
    out["merge_id_from_snapshot"] = _peak_mib(mem_merge_id_from_snapshot, ROWS_MERGE_ID, str(d))

    d = root / "row_hash"
    d.mkdir()
    out["get_row_hash_iterations"] = _peak_mib(
        mem_get_row_hash_iterations, ROWS_GET_ROW_HASH_ITER, str(d)
    )

    d = root / "reader_pq"
    d.mkdir()
    out["reader_get_parquet"] = _peak_mib(mem_reader_get_parquet, ROWS_READER_PARQUET, str(d))

    d = root / "reader_types"
    d.mkdir()
    out["reader_get_types_from_catalog"] = _peak_mib(
        mem_reader_get_types_from_catalog, COLS_READER_CATALOG_TYPES, str(d)
    )

    base = root / "pl_base"
    base.mkdir()
    out["polars_read_snapshots"] = _peak_mib(mem_pl_read_snapshots, N_LARGE, str(base), "snap")
    out["polars_snapshot_records_first_write"] = _peak_mib(
        mem_pl_snapshot_records_first_write, N_LARGE, str(base), "snap2"
    )
    out["polars_snapshot_records_merge"] = _peak_mib(
        mem_pl_snapshot_records_merge, ROWS_PL_MERGE, str(base), "snap3"
    )

    base = root / "lazy_base"
    base.mkdir()
    out["lazy_read_snapshots"] = _peak_mib(mem_lazy_read_snapshots, N_LARGE, str(base), "snap")
    out["lazy_snapshot_records_first_write"] = _peak_mib(
        mem_lazy_snapshot_records_first_write, N_LARGE, str(base), "snap2"
    )
    out["lazy_snapshot_records_merge"] = _peak_mib(
        mem_lazy_snapshot_records_merge, ROWS_LAZY_MERGE, str(base), "snap3"
    )

    d = root / "exp_pq"
    d.mkdir()
    out["to_export_pandas_parquet"] = _peak_mib(mem_to_export_pandas_parquet, N_LARGE, str(d))

    d = root / "exp_singer"
    d.mkdir()
    out["to_export_pandas_singer"] = _peak_mib(
        mem_to_export_pandas_singer, ROWS_EXPORT_SINGER, str(d)
    )

    d = root / "exp_pl_csv"
    d.mkdir()
    out["to_export_polars_csv"] = _peak_mib(mem_to_export_polars_csv, N_LARGE, str(d))

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Memory benchmark (peak MiB per scenario)")
    parser.add_argument(
        "--json",
        action="store_true",
        help="print one JSON object instead of text lines",
    )
    args = parser.parse_args()

    with TemporaryDirectory(prefix="gluestick_membench_") as tmp:
        root = Path(tmp)
        results = _run_scenarios(root)

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
    else:
        for name in sorted(results):
            print(f"{name}\t{results[name]:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
