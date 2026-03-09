"""Helpers for crash-safe snapshot writes via .lock rename.

Callers write to a .lock path; the canonical path only appears after an atomic
rename. This avoids leaving the canonical file truncated if the process is killed
mid-write. Orchestrators can ignore or delete files whose name ends with .lock.
"""

import os

LOCK_SUFFIX = ".lock"


def prepare_snapshot_write(canonical_path: str) -> str:
    """Prepare for writing a snapshot: move existing file to .lock path if present.

    Parameters
    ----------
    canonical_path : str
        The final path the snapshot should have (e.g. snapshot_dir/stream.snapshot.csv).

    Returns
    -------
    str
        The path to write to (canonical_path + ".lock"). If canonical_path existed,
        it has been renamed to this path so the same inode is reused.
    """
    lock_path = canonical_path + LOCK_SUFFIX
    if os.path.isfile(canonical_path):
        os.rename(canonical_path, lock_path)
    return lock_path


def finish_snapshot_write(lock_path: str, canonical_path: str) -> None:
    """Commit a snapshot write by renaming the .lock file to the canonical path.

    Call only after the write to lock_path has completed successfully.
    On failure, do not call this so the .lock file is left for the orchestrator
    to discard.

    Parameters
    ----------
    lock_path : str
        The path that was written to (canonical_path + ".lock").
    canonical_path : str
        The final path the snapshot should have.
    """
    os.rename(lock_path, canonical_path)
