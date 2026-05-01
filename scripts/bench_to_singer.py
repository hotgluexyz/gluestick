"""Benchmark optimized (current) vs original pre-optimization pandas_df_to_singer.

Runs each implementation in a fresh subprocess so memory/time numbers are clean,
then prints a side-by-side comparison.

Usage::

    PYTHONPATH=. python scripts/bench_to_singer.py [N_ROWS]

Both ``primitive`` and ``objects`` workloads are run by default.

The "ORIG" path is a verbatim copy of the pre-optimization
``pandas_df_to_singer`` body (taken from the git tree before the optimization
landed). The "FAST" path imports ``gluestick.singer.pandas_df_to_singer`` -
the integrated current version.
"""
from __future__ import annotations

import gc
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from contextlib import redirect_stdout
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
import singer

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gluestick.singer import (  # noqa: E402
    pandas_df_to_singer,
    gen_singer_header,
    get_catalog_schema,
    parse_df_cols,
    unwrap_json_schema,
    deep_convert_datetimes,
    remove_nulls_deep,
)


# ---------------------------------------------------------------------------
# Pre-optimization implementation (verbatim from HEAD before the change).
# ---------------------------------------------------------------------------


def pandas_df_to_singer_original(
    df: pd.DataFrame,
    stream,
    output_dir,
    keys=[],
    filename="data.singer",
    allow_objects=False,
    schema=None,
    unified_model=None,
    keep_null_fields=False,
    catalog_stream=None,
    trim_nested_nulls=False,
    recursive_typing=True,
):
    catalog_schema = os.environ.get("USE_CATALOG_SCHEMA", "false").lower() == "true"
    include_all_unified_fields = (
        os.environ.get("INCLUDE_ALL_UNIFIED_FIELDS", "false").lower() == "true"
        and unified_model is not None
    )

    if allow_objects and not (catalog_schema or include_all_unified_fields or keep_null_fields):
        df = df.dropna(how="all", axis=1)
    else:
        df = df.copy()

    if catalog_schema or catalog_stream:
        allow_objects = True
        stream_name = catalog_stream or stream
        schema = get_catalog_schema(stream_name)
        df = parse_df_cols(df, schema)
    elif unified_model:
        schema = unwrap_json_schema(unified_model.model_json_schema())

    df, header_map = gen_singer_header(
        df, allow_objects, schema, catalog_schema, recursive_typing=recursive_typing
    )
    output = os.path.join(output_dir, filename)
    mode = "a" if os.path.isfile(output) else "w"

    with open(output, mode) as f:
        with redirect_stdout(f):
            singer.write_schema(stream, header_map, keys)
            for _, row in df.iterrows():
                if not (catalog_schema or include_all_unified_fields or keep_null_fields):
                    filtered_row = row.dropna()
                else:
                    filtered_row = row.where(pd.notna(row), None)

                if trim_nested_nulls and not (
                    catalog_schema or include_all_unified_fields or keep_null_fields
                ):
                    filtered_row = remove_nulls_deep(filtered_row)
                else:
                    filtered_row = filtered_row.to_dict()

                filtered_row = deep_convert_datetimes(filtered_row)
                singer.write_record(stream, filtered_row)
            singer.write_state({})


# ---------------------------------------------------------------------------
# Workload + harness
# ---------------------------------------------------------------------------


def build_df(n_rows: int, mode: str) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    base = {
        "id": np.arange(n_rows, dtype=np.int64),
        "amount": rng.normal(100, 25, size=n_rows).astype(np.float64),
        "qty": rng.integers(0, 1000, size=n_rows, dtype=np.int64),
        "active": rng.integers(0, 2, size=n_rows, dtype=np.int8).astype(bool),
        "name": pd.array([f"name_{i % 10000}" for i in range(n_rows)], dtype="string"),
        "created_at": pd.to_datetime(
            rng.integers(1_600_000_000, 1_700_000_000, size=n_rows), unit="s", utc=True
        ),
    }
    if mode == "objects":
        # Includes a Timestamp inside a nested dict — covers the regression
        # the recent _json_default fix addresses.
        base["meta"] = [
            {"k": i % 5, "tag": f"t{i % 50}", "ts": pd.Timestamp("2024-01-01", tz="UTC")}
            for i in range(n_rows)
        ]
        base["tags"] = [[f"a{i % 7}", f"b{i % 11}"] for i in range(n_rows)]
        nulls = rng.random(n_rows) < 0.05
        notes = np.where(nulls, None, [f"note_{i}" for i in range(n_rows)])
        base["notes"] = notes
    return pd.DataFrame(base)


class RSSPoller(threading.Thread):
    def __init__(self, interval=0.05):
        super().__init__(daemon=True)
        self.proc = psutil.Process(os.getpid())
        self.interval = interval
        self.peak = 0
        self._stop_evt = threading.Event()

    def run(self):
        while not self._stop_evt.is_set():
            rss = self.proc.memory_info().rss
            if rss > self.peak:
                self.peak = rss
            self._stop_evt.wait(self.interval)

    def stop(self):
        self._stop_evt.set()
        self.join()


def fmt_mb(n_bytes: int) -> str:
    return f"{n_bytes / 1024 / 1024:,.1f} MB"


def run_one(label: str, fn, n_rows: int, mode: str) -> dict:
    df = build_df(n_rows, mode)
    proc = psutil.Process(os.getpid())
    gc.collect()
    rss_before = proc.memory_info().rss
    poller = RSSPoller()
    poller.start()
    tracemalloc.start()

    with tempfile.TemporaryDirectory() as tmp:
        t0 = time.time()
        fn(
            df=df,
            stream="bench",
            output_dir=tmp,
            keys=["id"],
            filename="out.singer",
            allow_objects=(mode == "objects"),
        )
        elapsed = time.time() - t0
        out_size = os.path.getsize(os.path.join(tmp, "out.singer"))

    _, tm_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    poller.stop()

    return {
        "label": label,
        "mode": mode,
        "n_rows": n_rows,
        "elapsed_s": round(elapsed, 2),
        "rss_peak_delta_mb": round((poller.peak - rss_before) / 1024 / 1024, 1),
        "tracemalloc_peak_mb": round(tm_peak / 1024 / 1024, 1),
        "output_mb": round(out_size / 1024 / 1024, 1),
        "rows_per_s": int(n_rows / elapsed),
    }


# Subprocess driver: each (impl, mode) runs in a fresh Python process
# so the memory measurements are independent.

CHILD_TEMPLATE = """\
import sys, json
sys.path.insert(0, {repo!r})
from scripts.bench_to_singer import run_one, pandas_df_to_singer_original
from gluestick.singer import pandas_df_to_singer
fn = pandas_df_to_singer if {impl!r} == 'fast' else pandas_df_to_singer_original
print(json.dumps(run_one({impl!r}, fn, {n_rows}, {mode!r})))
"""


def run_subprocess(impl: str, mode: str, n_rows: int) -> dict:
    code = CHILD_TEMPLATE.format(repo=str(REPO_ROOT), impl=impl, n_rows=n_rows, mode=mode)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    out = subprocess.check_output([sys.executable, "-c", code], env=env, stderr=subprocess.STDOUT)
    last_line = out.decode().strip().splitlines()[-1]
    return json.loads(last_line)


def print_table(rows):
    headers = ["mode", "impl", "rows", "time(s)", "rss Δ(MB)", "heap(MB)", "out(MB)", "rows/s"]
    widths = [max(len(h), 10) for h in headers]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("-" * (sum(widths) + 2 * (len(widths) - 1)))
    for r in rows:
        cells = [
            r["mode"],
            r["label"],
            f'{r["n_rows"]:,}',
            f'{r["elapsed_s"]}',
            f'{r["rss_peak_delta_mb"]}',
            f'{r["tracemalloc_peak_mb"]}',
            f'{r["output_mb"]}',
            f'{r["rows_per_s"]:,}',
        ]
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))


def main():
    n_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 500_000

    print(f"\n== Benchmarking pandas_df_to_singer at n_rows={n_rows:,} ==\n")
    rows = []
    for mode in ("primitive", "objects"):
        for impl in ("orig", "fast"):
            print(f"  running {mode}/{impl} ...", flush=True)
            rows.append(run_subprocess(impl, mode, n_rows))

    print()
    print_table(rows)
    print()

    # Speedup summary
    by_key = {(r["mode"], r["label"]): r for r in rows}
    print("== Speedup (fast vs orig) ==")
    for mode in ("primitive", "objects"):
        o = by_key[(mode, "orig")]
        f = by_key[(mode, "fast")]
        speedup = o["elapsed_s"] / max(f["elapsed_s"], 0.001)
        rss_ratio = f["rss_peak_delta_mb"] / max(o["rss_peak_delta_mb"], 0.001)
        heap_ratio = f["tracemalloc_peak_mb"] / max(o["tracemalloc_peak_mb"], 0.001)
        print(
            f"  {mode:10s}  time {speedup:.2f}x faster, "
            f"RSS Δ {rss_ratio*100:.0f}% of orig, heap {heap_ratio*100:.0f}% of orig"
        )

    # Save results JSON
    out_path = REPO_ROOT / ".benchmarks" / f"to_singer_{n_rows}.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"\nresults written to {out_path}")


if __name__ == "__main__":
    main()
