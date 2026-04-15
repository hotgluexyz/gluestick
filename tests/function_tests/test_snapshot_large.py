"""Large-dataset tests for `read_snapshots` and `snapshot_records` on Polars readers."""

import polars as pl

from gluestick.readers.pl_lazyframe_reader import PLLazyFrameReader
from gluestick.readers.pl_reader import PolarsReader

N_10K = 10_000
N_MERGE_NEW = 1_000


def _df_ids(start: int, count: int) -> pl.DataFrame:
    end = start + count
    return pl.DataFrame(
        {
            "id": list(range(start, end)),
            "v": [f"row_{i}" for i in range(start, end)],
        }
    )


def _lazy_ids(start: int, count: int) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "id": list(range(start, start + count)),
            "v": [f"row_{i}" for i in range(start, start + count)],
        }
    )


# ---- PolarsReader.read_snapshots ----
def test_read_snapshots_parquet_10k_rows(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    path = snapshot_dir / "orders.snapshot.parquet"
    _df_ids(0, N_10K).write_parquet(path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.read_snapshots("orders", str(snapshot_dir))

    assert df is not None
    assert df.height == N_10K
    assert df["id"][0] == 0
    assert df["id"][-1] == N_10K - 1
    assert df["v"][42] == "row_42"


def test_read_snapshots_csv_10k_rows(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    path = snapshot_dir / "orders.snapshot.csv"
    _df_ids(0, N_10K).write_csv(path)

    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))
    df = reader.read_snapshots("orders", str(snapshot_dir))

    assert df is not None
    assert df.height == N_10K
    assert df["id"][0] == 0
    assert df["id"][-1] == N_10K - 1


# ---- PLLazyFrameReader.read_snapshots ----
def test_lazy_read_snapshots_parquet_10k_rows(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _df_ids(0, N_10K).write_parquet(snapshot_dir / "orders.snapshot.parquet")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.read_snapshots("orders", str(snapshot_dir))

    assert lf is not None
    df = lf.collect()
    assert df.height == N_10K
    assert df["id"][0] == 0
    assert df["id"][-1] == N_10K - 1


def test_lazy_read_snapshots_csv_10k_rows(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _df_ids(0, N_10K).write_csv(snapshot_dir / "orders.snapshot.csv")

    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))
    lf = reader.read_snapshots("orders", str(snapshot_dir))

    assert lf is not None
    df = lf.collect()
    assert df.height == N_10K


# ---- PolarsReader.snapshot_records (large) ----
def test_snapshot_records_first_write_parquet_10k(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    stream_data = _df_ids(0, N_10K)
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(stream_data, "orders", str(snapshot_dir), pk="id")

    assert result is stream_data
    out = snapshot_dir / "orders.snapshot.parquet"
    assert out.is_file()
    assert pl.read_parquet(out).height == N_10K


def test_snapshot_records_merge_parquet_10k_plus_1k(tmp_path):
    """Existing 10k ids; incoming 1k new ids → 11k rows on disk and in merged result."""
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _df_ids(0, N_10K).write_parquet(snapshot_dir / "orders.snapshot.parquet")

    new_data = _df_ids(N_10K, N_MERGE_NEW)
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    merged = reader.snapshot_records(
        new_data, "orders", str(snapshot_dir), pk="id", just_new=False
    )

    assert merged is not None
    assert merged.height == N_10K + N_MERGE_NEW
    assert merged["id"].min() == 0
    assert merged["id"].max() == N_10K + N_MERGE_NEW - 1

    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.height == N_10K + N_MERGE_NEW
    assert loaded.sort("id")["id"][-1] == N_10K + N_MERGE_NEW - 1


def test_snapshot_records_merge_csv_5k_plus_1k(tmp_path):
    """CSV snapshots: 5k existing + 1k new keys (smaller than 10k parquet to limit CSV I/O time)."""
    n0, n1 = 5_000, 1_000
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _df_ids(0, n0).write_csv(snapshot_dir / "orders.snapshot.csv")

    new_data = _df_ids(n0, n1)
    reader = PolarsReader(dir=str(tmp_path), root=str(tmp_path))

    merged = reader.snapshot_records(
        new_data,
        "orders",
        str(snapshot_dir),
        pk="id",
        just_new=False,
        use_csv=True,
    )

    assert merged is not None
    assert merged.height == n0 + n1
    loaded = pl.read_csv(snapshot_dir / "orders.snapshot.csv")
    assert loaded.height == n0 + n1


# ---- PLLazyFrameReader.snapshot_records (large) ----
def test_lazy_snapshot_records_first_write_parquet_10k(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    lf_in = _lazy_ids(0, N_10K)
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))

    result = reader.snapshot_records(lf_in, "orders", str(snapshot_dir), pk="id")

    assert result is not None
    assert result.collect().height == N_10K
    assert (snapshot_dir / "orders.snapshot.parquet").is_file()
    assert pl.read_parquet(snapshot_dir / "orders.snapshot.parquet").height == N_10K


def test_lazy_snapshot_records_merge_parquet_10k_plus_1k(tmp_path):
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _df_ids(0, N_10K).write_parquet(snapshot_dir / "orders.snapshot.parquet")

    new_lf = _lazy_ids(N_10K, N_MERGE_NEW)
    reader = PLLazyFrameReader(dir=str(tmp_path), root=str(tmp_path))

    merged = reader.snapshot_records(
        new_lf, "orders", str(snapshot_dir), pk="id", just_new=False
    )

    assert merged is not None
    # Do not call merged.collect(): the returned LazyFrame may still reference the
    # snapshot path that was removed/renamed during sink+finish, which can break collection.
    loaded = pl.read_parquet(snapshot_dir / "orders.snapshot.parquet")
    assert loaded.height == N_10K + N_MERGE_NEW
    assert loaded["id"].max() == N_10K + N_MERGE_NEW - 1
