"""Shared workloads for memory smoke tests and offline benchmarking.

Import this from ``test_memory_usage.py`` and from ``scripts/memory_benchmark.py`` so
pytest and CLI runs exercise the **same** code paths and row counts.
"""

from __future__ import annotations

from pathlib import Path

# Scaling check (read_snapshots pandas): small vs large row counts.
N_SMALL = 8_000
N_LARGE = 55_000

# Fixed sizes for single-shot peak checks (keep in sync with test_memory_usage).
ROWS_MERGE_ETL = 22_000
ROWS_MERGE_ID = 12_000
ROWS_GET_ROW_HASH_ITER = 80_000
ROWS_READER_PARQUET = 70_000
COLS_READER_CATALOG_TYPES = 4_000
ROWS_PL_MERGE = 12_000
ROWS_LAZY_MERGE = 10_000
ROWS_EXPORT_SINGER = 18_000


# --- etl_utils (pandas) ---


def mem_read_snapshots_etl(n_rows: int, root: str) -> int:
    import pandas as pd

    from gluestick.etl_utils import read_snapshots

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"id": list(range(n_rows)), "v": [f"x{i}" for i in range(n_rows)]}
    ).to_parquet(p / "orders.snapshot.parquet", index=False)
    out = read_snapshots("orders", str(p))
    assert out is not None
    return len(out)


def mem_snapshot_records_merge_etl(n_rows: int, root: str) -> int:
    import pandas as pd

    from gluestick.etl_utils import snapshot_records

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"id": list(range(n_rows)), "v": ["a"] * n_rows}
    ).to_parquet(p / "orders.snapshot.parquet", index=False)
    delta = pd.DataFrame(
        {
            "id": list(range(n_rows, 2 * n_rows)),
            "v": ["b"] * n_rows,
        }
    )
    merged = snapshot_records(
        delta, "orders", str(p), pk="id", just_new=False
    )
    assert merged is not None
    return len(merged)


def mem_merge_id_from_snapshot(n_rows: int, root: str) -> int:
    import pandas as pd

    from gluestick.etl_utils import merge_id_from_snapshot

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    snap = pd.DataFrame(
        {
            "InputId": [f"ext-{i}" for i in range(n_rows)],
            "RemoteId": list(range(n_rows)),
        }
    )
    snap.to_parquet(p / "orders_flow1.snapshot.parquet", index=False)
    df = pd.DataFrame(
        {"externalId": [f"ext-{i}" for i in range(n_rows)], "x": list(range(n_rows))}
    )
    out = merge_id_from_snapshot(df, str(p), "orders", "flow1", "remote_pk")
    return len(out)


def mem_get_row_hash_iterations(n_iter: int, _root: str) -> int:
    import pandas as pd

    from gluestick.etl_utils import get_row_hash

    row = pd.Series({"b": 2, "a": 1})
    cols = ["a", "b"]
    h = None
    for _ in range(n_iter):
        h = get_row_hash(row, cols)
    assert h is not None
    return len(h)


# --- Reader (pandas) ---


def mem_reader_get_parquet(n_rows: int, root: str) -> int:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from gluestick.reader import Reader

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"id": list(range(n_rows)), "v": ["z"] * n_rows})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), p / "orders.parquet")
    reader = Reader(dir=str(p), root=str(p))
    out = reader.get("orders")
    return len(out)


def mem_reader_get_types_from_catalog(n_cols: int, root: str) -> int:
    Path(root).mkdir(parents=True, exist_ok=True)
    props = {f"c{i}": {"type": ["integer", "null"]} for i in range(n_cols)}
    catalog = {
        "streams": [
            {
                "stream": "s",
                "tap_stream_id": "s",
                "schema": {"type": "object", "properties": props},
            }
        ]
    }
    from gluestick.reader import Reader

    reader = Reader(dir=root, root=root)
    headers = [f"c{i}" for i in range(n_cols)]
    t = reader.get_types_from_catalog(catalog, "s", headers=headers)
    return len(t.get("dtype", {}))


# --- PolarsReader ---


def mem_pl_read_snapshots(n_rows: int, base: str, snap_rel: str) -> int:
    import polars as pl

    from gluestick.readers.pl_reader import PolarsReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"id": list(range(n_rows)), "v": [f"x{i}" for i in range(n_rows)]}
    ).write_parquet(snap / "orders.snapshot.parquet")
    reader = PolarsReader(dir=str(base_p), root=str(base_p))
    out = reader.read_snapshots("orders", str(snap))
    assert out is not None
    return out.height


def mem_pl_snapshot_records_first_write(n_rows: int, base: str, snap_rel: str) -> int:
    import polars as pl

    from gluestick.readers.pl_reader import PolarsReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    stream = pl.DataFrame(
        {"id": list(range(n_rows)), "v": ["a"] * n_rows}
    )
    reader = PolarsReader(dir=str(base_p), root=str(base_p))
    out = reader.snapshot_records(stream, "orders", str(snap), pk="id")
    assert out is not None
    return out.height


def mem_pl_snapshot_records_merge(n_per_side: int, base: str, snap_rel: str) -> int:
    import polars as pl

    from gluestick.readers.pl_reader import PolarsReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"id": list(range(n_per_side)), "v": ["a"] * n_per_side}
    ).write_parquet(snap / "orders.snapshot.parquet")
    new_data = pl.DataFrame(
        {
            "id": list(range(n_per_side, 2 * n_per_side)),
            "v": ["b"] * n_per_side,
        }
    )
    reader = PolarsReader(dir=str(base_p), root=str(base_p))
    out = reader.snapshot_records(
        new_data, "orders", str(snap), pk="id", just_new=False
    )
    assert out is not None
    return out.height


# --- PLLazyFrameReader ---


def mem_lazy_read_snapshots(n_rows: int, base: str, snap_rel: str) -> int:
    import polars as pl

    from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"id": list(range(n_rows)), "v": [f"x{i}" for i in range(n_rows)]}
    ).write_parquet(snap / "orders.snapshot.parquet")
    reader = PLLazyFrameReader(dir=str(base_p), root=str(base_p))
    lf = reader.read_snapshots("orders", str(snap))
    assert lf is not None
    return lf.collect().height


def mem_lazy_snapshot_records_first_write(n_rows: int, base: str, snap_rel: str) -> int:
    import polars as pl

    from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    lf_in = pl.LazyFrame(
        {"id": list(range(n_rows)), "v": ["a"] * n_rows}
    )
    reader = PLLazyFrameReader(dir=str(base_p), root=str(base_p))
    out = reader.snapshot_records(lf_in, "orders", str(snap), pk="id")
    assert out is not None
    return out.collect().height


def mem_lazy_snapshot_records_merge(n_per_side: int, base: str, snap_rel: str) -> int:
    """Validate merge via on-disk parquet; do not collect returned LazyFrame."""
    import polars as pl

    from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader

    base_p = Path(base)
    snap = base_p / snap_rel
    snap.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"id": list(range(n_per_side)), "v": ["a"] * n_per_side}
    ).write_parquet(snap / "orders.snapshot.parquet")
    new_lf = pl.LazyFrame(
        {
            "id": list(range(n_per_side, 2 * n_per_side)),
            "v": ["b"] * n_per_side,
        }
    )
    reader = PLLazyFrameReader(dir=str(base_p), root=str(base_p))
    merged = reader.snapshot_records(
        new_lf, "orders", str(snap), pk="id", just_new=False
    )
    assert merged is not None
    loaded = pl.read_parquet(snap / "orders.snapshot.parquet")
    return loaded.height


# --- to_export ---


def mem_to_export_pandas_parquet(n_rows: int, root: str) -> None:
    import pandas as pd

    from gluestick.etl_utils import to_export

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"id": list(range(n_rows)), "name": ["n"] * n_rows})
    to_export(
        df,
        name="stream",
        output_dir=str(p),
        keys=["id"],
        export_format="parquet",
    )


def mem_to_export_pandas_singer(n_rows: int, root: str) -> None:
    import pandas as pd

    from gluestick.etl_utils import to_export

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"id": list(range(n_rows)), "name": ["n"] * n_rows})
    to_export(
        df,
        name="orders",
        output_dir=str(p),
        keys=["id"],
        export_format="singer",
    )


def mem_to_export_polars_csv(n_rows: int, root: str) -> None:
    import polars as pl

    from gluestick.etl_utils import to_export

    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"id": list(range(n_rows)), "name": ["n"] * n_rows})
    to_export(df, name="c", output_dir=str(p), keys=["id"], export_format="csv")
