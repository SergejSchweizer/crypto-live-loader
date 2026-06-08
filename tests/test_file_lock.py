"""Tests for cross-process artifact file lock behavior."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ingestion.file_lock import locked_output_path


def test_locked_output_path_recovers_stale_pid_lock(tmp_path: Path) -> None:
    output_path = tmp_path / "artifact.parquet"
    lock_path = tmp_path / ".artifact.parquet.lock"
    lock_path.write_text("99999999", encoding="utf-8")

    with locked_output_path(output_path):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_locked_output_path_keeps_live_pid_lock(tmp_path: Path) -> None:
    output_path = tmp_path / "artifact.parquet"
    lock_path = tmp_path / ".artifact.parquet.lock"
    lock_path.write_text(str(os.getpid()), encoding="utf-8")

    with pytest.raises(TimeoutError):
        with locked_output_path(output_path, timeout_s=0.01):
            pass

    assert lock_path.exists()
