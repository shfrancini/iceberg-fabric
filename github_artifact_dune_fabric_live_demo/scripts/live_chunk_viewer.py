#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import html
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


DEFAULT_HEATMAP_COLOR_LIMITS = (-20.0, 50.0)
DEFAULT_WAVEFORM_WINDOW_SIZE = 24
DEFAULT_WAVEFORM_VERTICAL_SPACING = 16.0
PACKET_CHUNK_CAPTION = "Chunks are packet batches, not tick/time slices."
BENCHMARK_RIDGELINE_ADC_SCALE = 0.01
FULL_RANGE_WAVEFORM_VERTICAL_SPACING = 1.0
FULL_RANGE_WAVEFORM_LINEWIDTH = 0.48
FULL_RANGE_WAVEFORM_ALPHA = 0.95
FULL_RANGE_FIGURE_WIDTH = 11.2
FULL_RANGE_HEATMAP_FIGSIZE = (FULL_RANGE_FIGURE_WIDTH, 19.2)
FULL_RANGE_WAVEFORM_FIGSIZE = (FULL_RANGE_FIGURE_WIDTH, 19.2)
FULL_RANGE_WAVEFORM_Y_MARGIN = 4.0
FULL_RANGE_LAYOUT_PAD = 0.55
WAVEFORM_ACTIVITY_QUANTILE = 0.995


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def seconds_between(start: Any, end: Any) -> float | None:
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    if not start_dt or not end_dt:
        return None
    return round((end_dt - start_dt).total_seconds(), 3)


def decoded_chunk_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve() / "artifacts" / "decoded_chunks"


def live_visualization_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve() / "visualizations" / "live_chunks"


def timing_csv_path(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve() / "reports" / "live_visualization_timing.csv"


def list_chunk_csvs(run_dir_or_chunk_dir: str | Path) -> list[Path]:
    path = Path(run_dir_or_chunk_dir).expanduser().resolve()
    chunk_dir = path if path.name == "decoded_chunks" else decoded_chunk_dir(path)
    return sorted(chunk_dir.glob("chunk_*.csv"))


def read_manifest(run_dir: str | Path) -> dict[str, Any]:
    manifest_path = Path(run_dir).expanduser().resolve() / "reports" / "manifest.json"
    if not manifest_path.is_file():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"status": "manifest_unreadable", "path": str(manifest_path)}


def _load_cumulative_frame(chunk_csvs: Iterable[Path], max_rows: int):
    import pandas as pd

    frames = []
    remaining = max_rows
    for csv_path in chunk_csvs:
        if remaining <= 0:
            break
        try:
            frame = pd.read_csv(csv_path, nrows=remaining)
        except Exception:
            continue
        if not frame.empty:
            frames.append(frame)
            remaining -= len(frame)
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    for column in ("wire_id", "tick", "adc_value"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _inclusive_range(values: Sequence[int] | None, fallback_min: int, fallback_max: int) -> tuple[int, int]:
    if values is None:
        return fallback_min, fallback_max
    if len(values) != 2:
        raise ValueError("range values must be a 2-item sequence")
    first, second = int(values[0]), int(values[1])
    return (first, second) if first <= second else (second, first)


def _dark_figure(plt, figsize: tuple[float, float]):
    fig, ax = plt.subplots(figsize=figsize, facecolor="#080b0f")
    ax.set_facecolor("#05070a")
    for spine in ax.spines.values():
        spine.set_color("#6f7f8f")
    ax.tick_params(colors="#d6dde6", labelsize=9)
    ax.xaxis.label.set_color("#d6dde6")
    ax.yaxis.label.set_color("#d6dde6")
    ax.title.set_color("#f3f6f9")
    return fig, ax


def _select_contiguous_active_wires(centered_frame: Any, wire_min: int, wire_max: int, window_size: int) -> list[int]:
    """Select the contiguous wire window with the strongest pedestal-subtracted activity."""
    all_wires = list(range(wire_min, wire_max + 1))
    if not all_wires:
        return []
    bounded_size = max(1, min(int(window_size), len(all_wires)))
    if bounded_size == len(all_wires):
        return all_wires

    activity = (
        centered_frame.abs()
        .quantile(WAVEFORM_ACTIVITY_QUANTILE, axis=1)
        .reindex(all_wires)
        .fillna(0.0)
    )
    rolling = activity.rolling(window=bounded_size, min_periods=bounded_size).sum()
    valid = rolling.dropna()
    if valid.empty:
        start = wire_min
    else:
        end = int(valid.idxmax())
        start = end - bounded_size + 1
    return list(range(start, start + bounded_size))


def _sample_evenly_spaced_wires(wire_min: int, wire_max: int, sample_count: int) -> list[int]:
    all_wires = list(range(wire_min, wire_max + 1))
    if not all_wires:
        return []
    bounded_count = max(1, min(int(sample_count), len(all_wires)))
    if bounded_count == len(all_wires):
        return all_wires
    if bounded_count == 1:
        return [all_wires[len(all_wires) // 2]]

    max_index = len(all_wires) - 1
    indexes = [round(index * max_index / (bounded_count - 1)) for index in range(bounded_count)]
    return [all_wires[index] for index in dict.fromkeys(indexes)]


def _wire_tick_positions(selected_wires: Sequence[int], spacing: float, max_labels: int = 9) -> tuple[list[float], list[str]]:
    if not selected_wires:
        return [], []
    step = max(1, len(selected_wires) // max(1, max_labels - 1))
    indexes = list(range(0, len(selected_wires), step))
    if indexes[-1] != len(selected_wires) - 1:
        indexes.append(len(selected_wires) - 1)
    ticks = [index * spacing for index in indexes]
    labels = [str(selected_wires[index]) for index in indexes]
    return ticks, labels


def _wire_axis_tick_positions(wire_min: int, wire_max: int, max_labels: int = 21) -> tuple[list[int], list[str]]:
    wires = list(range(wire_min, wire_max + 1))
    if not wires:
        return [], []
    step = max(1, len(wires) // max(1, max_labels - 1))
    ticks = list(range(wire_min, wire_max + 1, step))
    if ticks[-1] != wire_max:
        ticks.append(wire_max)
    return ticks, [str(wire) for wire in ticks]


def _time_axis_tick_positions(tick_min: int, tick_max: int) -> list[int]:
    if tick_min == 0 and tick_max == 2047:
        return [0, 256, 512, 768, 1024, 1280, 1536, 1792, 2047]
    span = max(1, tick_max - tick_min)
    step = max(1, round(span / 8))
    ticks = list(range(tick_min, tick_max + 1, step))
    if ticks[-1] != tick_max:
        ticks.append(tick_max)
    return ticks


def render_cumulative_visualizations(
    chunk_csvs: Iterable[str | Path],
    output_dir: str | Path,
    *,
    label: str = "live chunks",
    max_rows: int = 500000,
    dpi: int = 120,
    expected_total_chunks: int | None = None,
    chunk_packets: int | None = None,
    total_packets: int | None = None,
    tick_range: Sequence[int] | None = None,
    wire_range: Sequence[int] | None = None,
    color_limits: Sequence[float] | None = None,
    waveform_mode: str = "full_range",
    waveform_wire_range: Sequence[int] | None = None,
    waveform_wires: Sequence[int] | None = None,
    waveform_window_size: int | None = None,
    waveform_adc_scale: float | None = None,
) -> dict[str, Any]:
    """Render cumulative heatmap and waveform from decoded chunk CSVs.

    The function preserves actual cumulative chunk order. It never creates a
    left-to-right mask; every rendered frame is built from the decoded rows in
    chunk_000.csv, then chunk_000.csv + chunk_001.csv, and so on.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    paths = [Path(path) for path in chunk_csvs]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    frame = _load_cumulative_frame(paths, max_rows=max_rows)
    chunk_count = len(paths)
    packets_rendered = chunk_count * int(chunk_packets) if chunk_packets else None
    if packets_rendered is not None and total_packets:
        packets_rendered = min(packets_rendered, int(total_packets))
    progress_label = f"chunk {chunk_count}"
    if expected_total_chunks:
        progress_label += f"/{int(expected_total_chunks)}"
    if packets_rendered is not None:
        progress_label += f", {packets_rendered}"
        progress_label += f"/{int(total_packets)} packets" if total_packets else " packets"
    if expected_total_chunks and packets_rendered is not None and total_packets:
        packet_chunk_progress = (
            f"Packet chunk progress: {chunk_count}/{int(expected_total_chunks)} chunks, "
            f"{packets_rendered}/{int(total_packets)} packets"
        )
    elif expected_total_chunks:
        packet_chunk_progress = f"Packet chunk progress: {chunk_count}/{int(expected_total_chunks)} chunks"
    elif packets_rendered is not None:
        packet_chunk_progress = f"Packet chunk progress: {chunk_count} chunks, {packets_rendered} packets"
    else:
        packet_chunk_progress = f"Packet chunk progress: {chunk_count} chunks"

    result: dict[str, Any] = {
        "chunk_count": chunk_count,
        "expected_total_chunks": expected_total_chunks,
        "chunk_packets": chunk_packets,
        "packets_rendered": packets_rendered,
        "total_packets": total_packets,
        "progress_label": progress_label,
        "packet_chunk_progress": packet_chunk_progress,
        "chunk_caption": PACKET_CHUNK_CAPTION,
        "rows": int(len(frame)),
        "output_dir": str(out),
        "heatmap": None,
        "stacked_waveform": None,
        "visualized_at": now_iso(),
    }
    required = {"wire_id", "tick", "adc_value"}
    if frame.empty or not required.issubset(frame.columns):
        result["error"] = "no decoded rows with wire_id/tick/adc_value are available yet"
        return result

    frame = frame.dropna(subset=["wire_id", "tick", "adc_value"])
    frame["wire_id"] = frame["wire_id"].astype(int)
    frame["tick"] = frame["tick"].astype(int)
    if frame.empty:
        result["error"] = "decoded rows exist, but no numeric wire_id/tick/adc_value rows are available"
        return result

    result["rows"] = int(len(frame))
    cumulative_wire_min = int(frame["wire_id"].min())
    cumulative_wire_max = int(frame["wire_id"].max())
    wire_min, wire_max = _inclusive_range(wire_range, cumulative_wire_min, cumulative_wire_max)
    tick_min, tick_max = _inclusive_range(tick_range, int(frame["tick"].min()), int(frame["tick"].max()))
    result.update(
        {
            "cumulative_chunk_count": chunk_count,
            "rendered_chunk_files": [path.name for path in paths],
            "heatmap_rows_used": int(len(frame)),
            "cumulative_data_wire_range": [cumulative_wire_min, cumulative_wire_max],
            "cumulative_data_wire_min": cumulative_wire_min,
            "cumulative_data_wire_max": cumulative_wire_max,
            "wire_range": [wire_min, wire_max],
            "tick_range": [tick_min, tick_max],
            "heatmap_origin": "lower",
            "wire_axis_orientation": f"wire_id increases upward ({wire_min} bottom -> {wire_max} top)",
        }
    )
    pivot = frame.pivot_table(index="wire_id", columns="tick", values="adc_value", aggfunc="mean")
    wire_index = pd.Index(range(wire_min, wire_max + 1), name="wire_id")
    tick_columns = pd.Index(range(tick_min, tick_max + 1), name="tick")
    pivot = pivot.reindex(index=wire_index, columns=tick_columns)
    pedestal = pivot.median(axis=1)
    centered = pivot.sub(pedestal, axis=0)
    if color_limits is None:
        vmin, vmax = DEFAULT_HEATMAP_COLOR_LIMITS
    else:
        vmin, vmax = float(color_limits[0]), float(color_limits[1])
    result.update(
        {
            "heatmap_value": "adc_minus_per_wire_median_pedestal",
            "heatmap_color_limits": [vmin, vmax],
            "heatmap_colorbar_label": "ADC - pedestal",
        }
    )

    heatmap_path = out / "live_heatmap.png"
    fig, ax = _dark_figure(plt, FULL_RANGE_HEATMAP_FIGSIZE)
    image = ax.imshow(
        centered.to_numpy(),
        aspect="auto",
        interpolation="nearest",
        origin="lower",
        cmap="magma",
        vmin=vmin,
        vmax=vmax,
        extent=[tick_min, tick_max, wire_min, wire_max],
    )
    heatmap_title = f"Event 4245 full heatmap - {progress_label}"
    heatmap_display_title = "Event 4245 heatmap"
    ax.set_title(heatmap_display_title, pad=4, fontsize=9)
    ax.set_xlabel("tick")
    ax.set_ylabel(f"wire_id ({wire_min} bottom -> {wire_max} top)")
    ax.set_xlim(tick_min, tick_max)
    ax.set_xticks(_time_axis_tick_positions(tick_min, tick_max))
    ax.set_ylim(wire_min, wire_max)
    heat_y_ticks, heat_y_labels = _wire_axis_tick_positions(wire_min, wire_max, max_labels=9)
    ax.set_yticks(heat_y_ticks)
    ax.set_yticklabels(heat_y_labels)
    ax.grid(color="#1b2530", alpha=0.35, linewidth=0.4)
    cbar = fig.colorbar(image, ax=ax, pad=0.008, fraction=0.022, aspect=35)
    cbar.set_label("ADC - pedestal", color="#d6dde6")
    cbar.ax.tick_params(colors="#d6dde6", labelsize=8)
    fig.tight_layout(pad=FULL_RANGE_LAYOUT_PAD)
    fig.savefig(heatmap_path, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)
    result["heatmap"] = str(heatmap_path)
    result["heatmap_title"] = heatmap_title

    waveform_path = out / "live_stacked_waveform.png"
    mode = str(waveform_mode or "full_range")
    if mode not in {"full_range", "zoom_window", "same_window_as_heatmap"}:
        raise ValueError("waveform_mode must be 'full_range', 'zoom_window', or 'same_window_as_heatmap'")

    if mode == "full_range":
        selected_wires = list(range(wire_min, wire_max + 1))
        waveform_selection = "full_wire_range"
    elif mode == "same_window_as_heatmap":
        selected_wires = _sample_evenly_spaced_wires(
            wire_min,
            wire_max,
            waveform_window_size or DEFAULT_WAVEFORM_WINDOW_SIZE,
        )
        waveform_selection = "same_window_as_heatmap_sample"
    elif waveform_wire_range is not None:
        waveform_min, waveform_max = _inclusive_range(waveform_wire_range, wire_min, wire_max)
        selected_wires = list(range(max(wire_min, waveform_min), min(wire_max, waveform_max) + 1))
        waveform_selection = "fixed_wire_window"
    elif waveform_wires is None:
        selected_wires = _select_contiguous_active_wires(
            centered,
            wire_min,
            wire_max,
            waveform_window_size or DEFAULT_WAVEFORM_WINDOW_SIZE,
        )
        waveform_selection = "strongest_contiguous_active_window"
    else:
        selected_wires = sorted({int(wire) for wire in waveform_wires if wire_min <= int(wire) <= wire_max})
        waveform_selection = "explicit_wire_list"
    waveform_rows_used = int(frame["wire_id"].isin(selected_wires).sum()) if selected_wires else 0
    if mode == "full_range":
        fig, ax = _dark_figure(plt, FULL_RANGE_WAVEFORM_FIGSIZE)
    else:
        fig, ax = _dark_figure(plt, (12.5, 7.2))
    fig.patch.set_facecolor("#0a0a0a")
    ax.set_facecolor("#0a0a0a")
    norm = plt.Normalize(vmin=wire_min, vmax=wire_max)
    colors = [plt.cm.turbo(norm(wire)) for wire in selected_wires]
    if mode == "full_range":
        spacing = FULL_RANGE_WAVEFORM_VERTICAL_SPACING
        waveform_linewidth = FULL_RANGE_WAVEFORM_LINEWIDTH
        waveform_alpha = FULL_RANGE_WAVEFORM_ALPHA
        waveform_scale = float(BENCHMARK_RIDGELINE_ADC_SCALE if waveform_adc_scale is None else waveform_adc_scale)
        waveform_centered = pivot.sub(pivot.mean(axis=1), axis=0)
        waveform_baseline = "wire_id"
        waveform_pedestal = "per_wire_mean"
    else:
        spacing = DEFAULT_WAVEFORM_VERTICAL_SPACING
        waveform_linewidth = 0.42
        waveform_alpha = 0.74
        waveform_scale = 1.0
        waveform_centered = centered
        waveform_baseline = "offset_index"
        waveform_pedestal = "per_wire_median"
    plotted = 0
    for offset, wire in enumerate(selected_wires):
        baseline = float(wire) if mode == "full_range" else float(offset * spacing)
        if wire not in waveform_centered.index:
            continue
        values = waveform_centered.loc[wire]
        if values.dropna().empty:
            continue
        ax.plot(
            tick_columns,
            values.to_numpy(dtype=float) * waveform_scale + baseline,
            linewidth=waveform_linewidth,
            color=colors[offset],
            alpha=waveform_alpha,
            solid_capstyle="round",
        )
        plotted += 1
    waveform_title_window = [selected_wires[0], selected_wires[-1]] if selected_wires else [wire_min, wire_max]
    if mode == "full_range" and waveform_title_window == [0, 199]:
        waveform_title = f"Event 4245 full wire trace view - {progress_label}"
    else:
        waveform_title = f"Event 4245 wire traces: wires {waveform_title_window[0]}-{waveform_title_window[1]} - {progress_label}"
    waveform_display_title = "Event 4245 ridgeline"
    ax.set_title(waveform_display_title, pad=4, fontsize=9)
    ax.set_xlabel("tick")
    ax.set_xlim(tick_min, tick_max)
    ax.set_xticks(_time_axis_tick_positions(tick_min, tick_max))
    if mode == "full_range":
        ax.set_ylabel("wire_id + scaled ADC deviation")
        ax.set_ylim(wire_min - FULL_RANGE_WAVEFORM_Y_MARGIN, wire_max + FULL_RANGE_WAVEFORM_Y_MARGIN)
        y_ticks, y_labels = _wire_axis_tick_positions(wire_min, wire_max, max_labels=21)
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels)
        ax.yaxis.grid(True, color="#ffffff", alpha=0.05, linewidth=0.35)
        ax.xaxis.grid(False)
    else:
        ax.set_ylabel("wire traces, vertically offset")
        ax.set_ylim(float(vmin) - spacing * 0.8, max(spacing, (len(selected_wires) - 1) * spacing + float(vmax) + spacing * 0.8))
        y_ticks, y_labels = _wire_tick_positions(selected_wires, spacing)
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels)
        ax.grid(color="#1b2530", alpha=0.38, linewidth=0.45)
    fig.tight_layout(pad=FULL_RANGE_LAYOUT_PAD)
    fig.savefig(waveform_path, dpi=dpi, facecolor=fig.get_facecolor())
    plt.close(fig)
    waveform_wire_window = [selected_wires[0], selected_wires[-1]] if selected_wires else None
    debug_metadata = {
        "cumulative_chunk_count": chunk_count,
        "rendered_chunk_files": [path.name for path in paths],
        "packet_chunk_progress": packet_chunk_progress,
        "chunk_caption": result["chunk_caption"],
        "heatmap_rows_used": int(result["heatmap_rows_used"]),
        "waveform_rows_used": waveform_rows_used,
        "waveform_wire_range": waveform_wire_window,
        "cumulative_data_wire_range": [cumulative_wire_min, cumulative_wire_max],
        "waveform_mode": mode,
        "waveform_selection": waveform_selection,
        "waveform_linewidth": waveform_linewidth,
        "waveform_alpha": waveform_alpha,
        "waveform_colormap": "turbo",
        "waveform_adc_scale": waveform_scale,
        "waveform_baseline": waveform_baseline,
        "waveform_pedestal": waveform_pedestal,
    }
    result["stacked_waveform"] = str(waveform_path)
    result["waveform_title"] = waveform_title
    result["waveform_value"] = f"{waveform_baseline} + {waveform_scale:g} * adc_minus_{waveform_pedestal}_pedestal"
    result["waveform_mode"] = mode
    result["waveform_selection"] = waveform_selection
    result["waveform_wire_window"] = waveform_wire_window
    result["waveform_rows_used"] = waveform_rows_used
    result["waveform_wires"] = selected_wires
    result["waveform_plotted_traces"] = plotted
    result["waveform_vertical_spacing_adc"] = spacing
    result["waveform_adc_scale"] = waveform_scale
    result["waveform_baseline"] = waveform_baseline
    result["waveform_pedestal"] = waveform_pedestal
    result["debug_metadata"] = debug_metadata
    result["visualized_at"] = now_iso()
    return result


def _chunk_index_from_path(path: Path) -> int | None:
    match = re.search(r"chunk_(\d+)\.csv$", path.name)
    return int(match.group(1)) if match else None


def _manifest_chunks_by_index(manifest: dict[str, Any]) -> dict[int, dict[str, Any]]:
    chunks: dict[int, dict[str, Any]] = {}
    for chunk in manifest.get("chunks") or []:
        try:
            chunks[int(chunk.get("chunk_index"))] = chunk
        except (TypeError, ValueError):
            continue
    return chunks


def _mtime_iso(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat()
    except FileNotFoundError:
        return None


def _write_timing_csv(path: Path, records: dict[int, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "chunk",
        "packets",
        "decoded_rows",
        "rows_appended",
        "receiver_capture_completed_at",
        "decode_started_at",
        "decode_completed_at",
        "local_chunk_csv_available_at",
        "visualized_at",
        "capture_to_decode_s",
        "decode_duration_s",
        "receiver_to_local_copy_s",
        "visualization_lag_s",
        "capture_to_visualization_lag_s",
        "chunk_csv_mtime",
        "chunk_csv_available_at",
        "capture_completed_at",
        "decode_to_visualization_lag_s",
        "queue_depth",
        "validation_pass",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index in sorted(records):
            writer.writerow({field: records[index].get(field) for field in fields})


def update_timing_records(
    run_dir: str | Path,
    chunk_csvs: Sequence[Path],
    manifest: dict[str, Any],
    render_result: dict[str, Any],
    records: dict[int, dict[str, Any]],
) -> Path:
    """Record first visualization timing per chunk and write the timing CSV."""
    manifest_chunks = _manifest_chunks_by_index(manifest)
    queue_depth = (manifest.get("queue_metrics") or {}).get("queue_depth_chunks")
    validation_pass = (manifest.get("validation_result") or {}).get("pass")
    total_packets = render_result.get("total_packets")
    chunk_packets = render_result.get("chunk_packets") or 64
    visualized_at = render_result.get("visualized_at") or now_iso()
    for csv_path in chunk_csvs:
        index = _chunk_index_from_path(csv_path)
        if index is None:
            continue
        manifest_chunk = manifest_chunks.get(index, {})
        receiver_capture_completed_at = (
            manifest_chunk.get("receiver_capture_completed_at")
            or manifest_chunk.get("capture_completed_time")
        )
        decode_started_at = manifest_chunk.get("decode_started_at")
        decode_completed_at = (
            manifest_chunk.get("decode_completed_at")
            or manifest_chunk.get("decode_completed_time")
        )
        local_copy_at = (
            manifest_chunk.get("local_chunk_csv_available_at")
            or manifest_chunk.get("local_copy_completed_time")
        )
        csv_mtime = _mtime_iso(csv_path)
        available_at = local_copy_at or csv_mtime
        packets_done = (index + 1) * int(chunk_packets)
        if total_packets:
            packets_done = min(packets_done, int(total_packets))
            packet_text = f"{packets_done}/{int(total_packets)}"
        else:
            packet_text = str(packets_done)
        if index not in records:
            records[index] = {
                "chunk": index,
                "packets": packet_text,
                "decoded_rows": render_result.get("rows"),
                "visualized_at": visualized_at,
                "receiver_capture_completed_at": receiver_capture_completed_at,
                "decode_started_at": decode_started_at,
                "decode_completed_at": decode_completed_at,
                "local_chunk_csv_available_at": available_at,
                "chunk_csv_available_at": available_at,
                "chunk_csv_mtime": csv_mtime,
                "capture_to_decode_s": seconds_between(receiver_capture_completed_at, decode_started_at),
                "decode_duration_s": seconds_between(decode_started_at, decode_completed_at),
                "receiver_to_local_copy_s": seconds_between(decode_completed_at, available_at),
                "visualization_lag_s": seconds_between(available_at, visualized_at),
                "capture_to_visualization_lag_s": seconds_between(receiver_capture_completed_at, visualized_at),
                "decode_to_visualization_lag_s": seconds_between(decode_completed_at, visualized_at),
            }
        record = records[index]
        local_available_at = local_copy_at or record.get("local_chunk_csv_available_at") or available_at
        record.update(
            {
                "packets": record.get("packets") or packet_text,
                "rows_appended": manifest_chunk.get("rows_appended", record.get("rows_appended")),
                "receiver_capture_completed_at": record.get("receiver_capture_completed_at") or receiver_capture_completed_at,
                "decode_started_at": record.get("decode_started_at") or decode_started_at,
                "decode_completed_at": record.get("decode_completed_at") or decode_completed_at,
                "local_chunk_csv_available_at": local_available_at,
                "chunk_csv_available_at": local_available_at,
                "chunk_csv_mtime": record.get("chunk_csv_mtime") or csv_mtime,
                "capture_completed_at": record.get("capture_completed_at") or receiver_capture_completed_at,
                "queue_depth": queue_depth,
                "validation_pass": validation_pass,
            }
        )
        duration_inputs = {
            "capture_to_decode_s": (record.get("receiver_capture_completed_at"), record.get("decode_started_at")),
            "decode_duration_s": (record.get("decode_started_at"), record.get("decode_completed_at")),
            "receiver_to_local_copy_s": (record.get("decode_completed_at"), record.get("local_chunk_csv_available_at")),
            "visualization_lag_s": (record.get("local_chunk_csv_available_at"), record.get("visualized_at")),
            "capture_to_visualization_lag_s": (record.get("receiver_capture_completed_at"), record.get("visualized_at")),
            "decode_to_visualization_lag_s": (record.get("decode_completed_at"), record.get("visualized_at")),
        }
        for field, (start, end) in duration_inputs.items():
            record[field] = seconds_between(start, end)
    output_path = timing_csv_path(run_dir)
    _write_timing_csv(output_path, records)
    return output_path


def _png_data_uri(path: str | Path) -> str:
    data = Path(path).read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def side_by_side_images_html(heatmap_path: str | Path, waveform_path: str | Path, progress_label: str | None = None) -> str:
    heatmap = Path(heatmap_path)
    waveform = Path(waveform_path)
    missing = [str(path) for path in (heatmap, waveform) if not path.is_file()]
    if missing:
        items = "".join(f"<li><code>{html.escape(path)}</code></li>" for path in missing)
        return f"<div style='color:#d6dde6;background:#080b0f;padding:10px;border-radius:8px'>Waiting for image files:<ul>{items}</ul></div>"
    heatmap_uri = _png_data_uri(heatmap)
    waveform_uri = _png_data_uri(waveform)
    label_suffix = f" — {html.escape(str(progress_label))}" if progress_label else ""
    return (
        "<div style='display:flex;gap:10px;align-items:flex-start;justify-content:center;"
        "width:100%;max-width:1100px;margin:0 auto;background:#080b0f;padding:8px;box-sizing:border-box;overflow:hidden;'>"
        "<div style='flex:0 1 49%;max-width:49%;min-width:0;text-align:center;color:#f3f6f9;font-family:sans-serif;box-sizing:border-box;'>"
        f"<div style='font-weight:700;margin:0 0 6px 0;'>Heatmap{label_suffix}</div>"
        f"<img src='{heatmap_uri}' style='width:100%;height:auto;object-fit:contain;display:block;border:1px solid #263442;border-radius:4px;' />"
        "</div>"
        "<div style='flex:0 1 49%;max-width:49%;min-width:0;text-align:center;color:#f3f6f9;font-family:sans-serif;box-sizing:border-box;'>"
        f"<div style='font-weight:700;margin:0 0 6px 0;'>Ridgeline{label_suffix}</div>"
        f"<img src='{waveform_uri}' style='width:100%;height:auto;object-fit:contain;display:block;border:1px solid #263442;border-radius:4px;' />"
        "</div>"
        "</div>"
    )


def _safe_display_update(handle: Any, obj: Any) -> None:
    if hasattr(handle, "update"):
        handle.update(obj)
        return
    try:
        from IPython.display import display

        display(obj)
    except Exception:
        # Headless validation has no display publisher; rendering files is still useful.
        return


def _status_html(run_dir: Path, manifest: dict[str, Any], chunk_count: int, last_result: dict[str, Any] | None) -> str:
    status = manifest.get("status") or "waiting"
    phase = manifest.get("phase") or "waiting_for_manifest"
    validation = manifest.get("validation_result") or {}
    queue = manifest.get("queue_metrics") or {}
    rows = "n/a" if not last_result else str(last_result.get("rows", "n/a"))
    progress = "waiting for chunks" if not last_result else str(last_result.get("progress_label") or f"chunk {chunk_count}")
    packet_chunk_progress = "Packet chunk progress: waiting for decoded chunks"
    chunk_caption = PACKET_CHUNK_CAPTION
    if last_result:
        packet_chunk_progress = str(last_result.get("packet_chunk_progress") or packet_chunk_progress)
        chunk_caption = str(last_result.get("chunk_caption") or chunk_caption)
    passed = manifest.get("pass")
    validation_pass = validation.get("pass") if isinstance(validation, dict) else None
    debug_html = ""
    if last_result:
        debug = last_result.get("debug_metadata") or {}
        chunk_files = debug.get("rendered_chunk_files") or []
        if chunk_files:
            chunk_prefix = f"{chunk_files[0]} through {chunk_files[-1]}"
        else:
            chunk_prefix = "n/a"
        waveform_delta = last_result.get("waveform_rows_added_since_last_update")
        waveform_delta_text = "n/a" if waveform_delta is None else f"{int(waveform_delta):+d}"
        waveform_note = last_result.get("waveform_rows_added_note")
        note_html = f"<br><b style='color:#ffd166'>{html.escape(str(waveform_note))}</b>" if waveform_note else ""
        debug_html = (
            f"Render prefix: <code>{html.escape(str(chunk_prefix))}</code><br>"
            f"Render debug: chunks=<b>{html.escape(str(debug.get('cumulative_chunk_count', 'n/a')))}</b> "
            f"heatmap_rows=<b>{html.escape(str(debug.get('heatmap_rows_used', 'n/a')))}</b> "
            f"waveform_rows=<b>{html.escape(str(debug.get('waveform_rows_used', 'n/a')))}</b> "
            f"waveform_delta=<b>{html.escape(waveform_delta_text)}</b><br>"
            f"Wire coverage: data=<code>{html.escape(str(debug.get('cumulative_data_wire_range', 'n/a')))}</code> "
            f"waveform=<code>{html.escape(str(debug.get('waveform_wire_range', 'n/a')))}</code> "
            f"mode=<code>{html.escape(str(debug.get('waveform_mode', 'n/a')))}</code>{note_html}<br>"
        )
    return (
        "<div style='font-family: sans-serif; line-height: 1.35; background:#080b0f; color:#d6dde6; "
        "border:1px solid #304050; border-radius:8px; padding:10px'>"
        "<b style='color:#f3f6f9'>Live chunk-by-chunk demo</b><br>"
        f"<div style='margin:6px 0; padding:8px; background:#111923; border-radius:6px;'>"
        f"<b style='color:#ffd166'>{html.escape(packet_chunk_progress)}</b><br>"
        f"<span>{html.escape(chunk_caption)}</span>"
        f"</div>"
        f"Run directory: <code>{html.escape(str(run_dir))}</code><br>"
        f"Progress: <b style='color:#ffd166'>{html.escape(progress)}</b><br>"
        f"Status: <b>{html.escape(str(status))}</b> &nbsp; Phase: <b>{html.escape(str(phase))}</b><br>"
        f"Decoded chunk CSVs visible locally: <b>{chunk_count}</b> &nbsp; Cumulative rendered rows: <b>{html.escape(rows)}</b><br>"
        f"{debug_html}"
        f"Queue metrics: <code>{html.escape(json.dumps(queue, sort_keys=True))}</code><br>"
        f"Run pass: <b>{html.escape(str(passed))}</b> &nbsp; Validation pass: <b>{html.escape(str(validation_pass))}</b>"
        "</div>"
    )


def start_notebook_live_viewer(
    run_dir: str | Path,
    *,
    poll_seconds: float = 2.0,
    max_rows: int = 500000,
    label: str = "live chunks",
    stop_when_complete: bool = True,
    expected_total_chunks: int | None = None,
    chunk_packets: int | None = None,
    total_packets: int | None = None,
    tick_range: Sequence[int] | None = None,
    wire_range: Sequence[int] | None = None,
    color_limits: Sequence[float] | None = None,
    waveform_mode: str = "full_range",
    waveform_wire_range: Sequence[int] | None = None,
    waveform_wires: Sequence[int] | None = None,
    waveform_window_size: int | None = None,
    waveform_adc_scale: float | None = None,
) -> dict[str, Any]:
    """Start a background notebook display watcher for decoded chunk CSVs."""
    from IPython.display import HTML, display

    resolved_run_dir = Path(run_dir).expanduser().resolve()
    output_dir = live_visualization_dir(resolved_run_dir)
    stop_event = threading.Event()
    timing_records: dict[int, dict[str, Any]] = {}
    state: dict[str, Any] = {
        "run_dir": str(resolved_run_dir),
        "output_dir": str(output_dir),
        "timing_csv": str(timing_csv_path(resolved_run_dir)),
        "updates": 0,
        "rendered_chunk_counts": [],
        "last_result": None,
        "last_error": None,
    }

    status_handle = display(HTML(_status_html(resolved_run_dir, {}, 0, None)), display_id=True)
    image_pair_handle = display(HTML("<em>Waiting for decoded chunk CSVs...</em>"), display_id=True)

    def worker() -> None:
        last_signature: tuple[tuple[str, int, int], ...] | None = None
        last_rendered_count = 0
        while not stop_event.is_set():
            manifest = read_manifest(resolved_run_dir)
            csvs = list_chunk_csvs(resolved_run_dir)
            signature = tuple((path.name, path.stat().st_size, path.stat().st_mtime_ns) for path in csvs)
            if csvs and signature != last_signature:
                try:
                    render_counts = list(range(last_rendered_count + 1, len(csvs) + 1))
                    if not render_counts:
                        render_counts = [len(csvs)]
                    for prefix_count in render_counts:
                        prefix_csvs = csvs[:prefix_count]
                        result = render_cumulative_visualizations(
                            prefix_csvs,
                            output_dir,
                            label=label,
                            max_rows=max_rows,
                            expected_total_chunks=expected_total_chunks,
                            chunk_packets=chunk_packets,
                            total_packets=total_packets,
                            tick_range=tick_range,
                            wire_range=wire_range,
                            color_limits=color_limits,
                            waveform_mode=waveform_mode,
                            waveform_wire_range=waveform_wire_range,
                            waveform_wires=waveform_wires,
                            waveform_window_size=waveform_window_size,
                            waveform_adc_scale=waveform_adc_scale,
                        )
                        previous_result = state.get("last_result") if isinstance(state.get("last_result"), dict) else None
                        previous_waveform_rows = None if not previous_result else previous_result.get("waveform_rows_used")
                        if previous_waveform_rows is None:
                            waveform_delta = None
                        else:
                            waveform_delta = int(result.get("waveform_rows_used") or 0) - int(previous_waveform_rows or 0)
                        result["waveform_rows_added_since_last_update"] = waveform_delta
                        waveform_note = None
                        if waveform_delta == 0 and previous_result and result.get("chunk_count") != previous_result.get("chunk_count"):
                            if result.get("waveform_mode") == "full_range":
                                waveform_note = "No new decoded rows were added for this chunk; both plots may remain unchanged."
                            else:
                                waveform_note = "No new rows landed in the selected waveform wires for this chunk; the heatmap may still change outside the waveform window."
                        elif waveform_delta is not None and 0 < waveform_delta < 512:
                            waveform_note = "Only a few new rows landed in the selected waveform wires for this chunk."
                        if waveform_note:
                            result["waveform_rows_added_note"] = waveform_note
                        if isinstance(result.get("debug_metadata"), dict):
                            result["debug_metadata"]["waveform_rows_added_since_last_update"] = waveform_delta
                            result["debug_metadata"]["waveform_rows_added_note"] = waveform_note
                        state["updates"] = int(state["updates"]) + 1
                        state["rendered_chunk_counts"].append(prefix_count)
                        state["last_result"] = result
                        state["last_error"] = result.get("error")
                        timing_path = update_timing_records(resolved_run_dir, prefix_csvs, manifest, result, timing_records)
                        state["timing_csv"] = str(timing_path)
                        _safe_display_update(status_handle, HTML(_status_html(resolved_run_dir, manifest, prefix_count, result)))
                        if result.get("heatmap") and result.get("stacked_waveform"):
                            _safe_display_update(
                                image_pair_handle,
                                HTML(
                                    side_by_side_images_html(
                                        result["heatmap"],
                                        result["stacked_waveform"],
                                        progress_label=str(result.get("progress_label") or ""),
                                    )
                                ),
                            )
                        last_rendered_count = max(last_rendered_count, prefix_count)
                except Exception as error:  # keep the watcher alive for transient partial-file reads
                    state["last_error"] = str(error)
                    _safe_display_update(
                        status_handle,
                        HTML(_status_html(resolved_run_dir, manifest, len(csvs), state.get("last_result")) + f"<pre>{html.escape(str(error))}</pre>"),
                    )
                last_signature = signature
            else:
                _safe_display_update(status_handle, HTML(_status_html(resolved_run_dir, manifest, len(csvs), state.get("last_result"))))

            if stop_when_complete and manifest.get("status") in {"complete", "failed"}:
                break
            time.sleep(poll_seconds)

        manifest = read_manifest(resolved_run_dir)
        csvs = list_chunk_csvs(resolved_run_dir)
        if csvs and state.get("last_result"):
            timing_path = update_timing_records(resolved_run_dir, csvs, manifest, state["last_result"], timing_records)
            state["timing_csv"] = str(timing_path)
        _safe_display_update(status_handle, HTML(_status_html(resolved_run_dir, manifest, len(csvs), state.get("last_result"))))

    thread = threading.Thread(target=worker, name="dune-live-chunk-viewer", daemon=True)
    thread.start()
    return {"thread": thread, "stop_event": stop_event, "state": state}
