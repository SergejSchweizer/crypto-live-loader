"""Shared helpers for incremental input-change detection."""

from __future__ import annotations

from pathlib import Path

from ingestion.artifact_state import FileFingerprint


def inputs_unchanged(
    previous_fingerprints: object,
    current_fingerprints: dict[str, FileFingerprint],
) -> bool:
    """Return whether fingerprint maps are equal and structurally valid."""

    return isinstance(previous_fingerprints, dict) and previous_fingerprints == current_fingerprints


def changed_input_files(
    files: list[Path],
    previous_fingerprints: object,
    current_fingerprints: dict[str, FileFingerprint],
    include_all: bool = False,
) -> list[Path]:
    """Return input files whose content changed or must be rebuilt."""

    if include_all or not isinstance(previous_fingerprints, dict):
        return list(files)

    deleted_inputs = set(previous_fingerprints) - set(current_fingerprints)
    if deleted_inputs:
        return list(files)

    changed: list[Path] = []
    for path in files:
        resolved_path = str(path.resolve())
        if previous_fingerprints.get(resolved_path) != current_fingerprints[resolved_path]:
            changed.append(path)
    return changed
