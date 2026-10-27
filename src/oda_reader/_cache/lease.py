"""Leases that keep a payload's derived Parquet files from automatic deletion.

A lease is an exclusive lock on a lease file of its own,
``.{key}-{digest}.{uuid}.lease`` beside the derived file. A deleter locks
each lease file it finds without waiting: one it cannot lock belongs to a live
reader, so the file stays; one it can lock belongs to a reader that died, so
the lease file goes. The operating system releases the lock of a process that
exits, so a crash needs no cleanup. Taking a lease and a deleter's check are
serialised by ``.{key}.lease.lock``, so no file is deleted between a reader's
lock and its first read.
"""

import contextlib
import uuid
from pathlib import Path

from filelock import FileLock, Timeout


def derived_files(payload: Path) -> list[Path]:
    """The ``{key}-*.parquet`` files made from *payload*."""
    return list(payload.parent.glob(f"{payload.stem}-*.parquet"))


def lease_guard(payload: Path) -> FileLock:
    """The lock that orders taking a lease against a deleter's check of leases."""
    return FileLock(str(payload.with_name(f".{payload.stem}.lease.lock")))


def take_lease(payload: Path, dest: Path) -> FileLock:
    """Lock a new lease file for *dest*, which need not exist yet."""
    lock = FileLock(
        str(dest.with_name(f".{dest.stem}.{uuid.uuid4().hex}.lease")),
        # A lease taken in one thread may be closed in another.
        thread_local=False,
    )
    with lease_guard(payload):
        lock.acquire()
    return lock


def release_lease(lock: FileLock) -> None:
    lock.release()
    # Windows removes the lock file on release; elsewhere it stays.
    with contextlib.suppress(OSError):
        Path(lock.lock_file).unlink()


def leased(derived: Path) -> bool:
    """Whether a live reader leases *derived*. Remove dead readers' lease files.

    Hold the lease guard.
    """
    held = False
    for path in derived.parent.glob(f".{derived.stem}.*.lease"):
        lock = FileLock(str(path), timeout=0, thread_local=False)
        try:
            lock.acquire()
        except Timeout:
            held = True
        else:
            release_lease(lock)
    return held
