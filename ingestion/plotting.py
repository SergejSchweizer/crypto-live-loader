"""Shared plotting helpers for compact lake artifact profiles."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def write_compact_feature_profile_png(
    *,
    frame: pl.DataFrame,
    path: Path,
    x_column: str,
    y_columns: list[str],
    color: str,
    marker: str | None = None,
) -> None:
    """Write a compact stacked line-profile PNG for selected numeric columns."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    present = [column for column in y_columns if column in frame.columns]
    if not present or x_column not in frame.columns:
        path.touch()
        return

    fig, axes = plt.subplots(len(present), 1, figsize=(12, max(4, len(present) * 1.8)), squeeze=False)
    fig.patch.set_facecolor("#111217")
    x_values = frame[x_column].to_list()
    plot_kwargs = {"color": color, "linewidth": 0.9}
    if marker is not None:
        plot_kwargs.update({"marker": marker, "markersize": 2.5})

    for index, column in enumerate(present):
        axis = axes[index][0]
        axis.set_facecolor("#161922")
        axis.plot(x_values, frame[column].to_list(), **plot_kwargs)
        axis.set_title(column, color="#eceff4", fontsize=9, loc="left")
        axis.tick_params(colors="#d8dee9", labelsize=7)

    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)
