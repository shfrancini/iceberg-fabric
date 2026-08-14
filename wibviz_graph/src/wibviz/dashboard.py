import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Snapshot:
    figure: object
    rows: int
    wires: int
    min_tick: int
    max_tick: int


def _dashboard_dependencies():
    try:
        import numpy as np
        import pandas as pd
        import plotly.graph_objects as go
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Dashboard dependencies are missing. Install them with "
            "'pip install -e .[dashboard]'."
        ) from error
    return np, pd, go


def empty_figure(message):
    _, _, go = _dashboard_dependencies()
    figure = go.Figure()
    figure.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"size": 20},
    )
    figure.update_layout(template="plotly_dark", height=800)
    return figure


def load_snapshot(csv_path, tpc=0, plane=0):
    np, pd, go = _dashboard_dependencies()
    csv_path = Path(csv_path)
    if not csv_path.is_file() or csv_path.stat().st_size == 0:
        raise ValueError(f"CSV is not available yet: {csv_path}")

    try:
        data = pd.read_csv(
            csv_path,
            usecols=["tick", "adc_value", "wire_id", "tpc", "plane"],
            on_bad_lines="skip",
        )
    except pd.errors.EmptyDataError as error:
        raise ValueError(f"CSV has no complete rows yet: {csv_path}") from error

    data = data.dropna(subset=["tick", "adc_value", "wire_id", "tpc", "plane"])
    data = data[(data["tpc"] == tpc) & (data["plane"] == plane)]
    if data.empty:
        raise ValueError(f"No complete rows found for TPC={tpc}, plane={plane}")

    data = data.astype(
        {"tick": int, "wire_id": int, "tpc": int, "plane": int, "adc_value": float}
    )
    min_tick = int(data["tick"].min())
    max_tick = int(data["tick"].max())
    ticks = np.arange(min_tick, max_tick + 1)

    pivot = (
        data.groupby(["wire_id", "tick"])["adc_value"]
        .max()
        .unstack()
        .sort_index()
        .reindex(columns=ticks)
    )
    values = pivot.to_numpy(dtype=float)
    pedestals = np.nanmedian(values, axis=1, keepdims=True)
    normalized = values - pedestals

    figure = go.Figure(
        go.Heatmap(
            z=normalized,
            x=ticks,
            y=pivot.index.to_numpy(),
            colorscale="Thermal",
            zmin=-20,
            zmax=50,
            colorbar={"title": "ADC - pedestal"},
            hovertemplate=(
                "Wire: %{y}<br>"
                "Tick: %{x}<br>"
                "ADC - pedestal: %{z:.1f}"
                "<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title=f"DUNE/FABRIC live replay — TPC {tpc}, plane {plane}",
        xaxis_title="Time tick",
        yaxis_title="Wire ID",
        template="plotly_dark",
        height=850,
        margin={"l": 70, "r": 40, "t": 80, "b": 70},
        uirevision="preserve-user-zoom",
    )
    figure.update_yaxes(autorange="reversed")

    return Snapshot(
        figure=figure,
        rows=len(data),
        wires=len(pivot.index),
        min_tick=min_tick,
        max_tick=max_tick,
    )


def build_app(csv_path, tpc=0, plane=0, refresh_seconds=2.0):
    try:
        from dash import Dash, Input, Output, dcc, html, no_update
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Dash is missing. Install dashboard dependencies with "
            "'pip install -e .[dashboard]'."
        ) from error

    csv_path = Path(csv_path)
    app = Dash(__name__)
    cache = {"signature": None}

    try:
        initial = load_snapshot(csv_path, tpc=tpc, plane=plane)
        initial_figure = initial.figure
        initial_status = (
            f"{initial.rows:,} samples | {initial.wires} wires | "
            f"ticks {initial.min_tick}-{initial.max_tick}"
        )
        stat = csv_path.stat()
        cache["signature"] = (stat.st_size, stat.st_mtime_ns)
    except (OSError, ValueError) as error:
        initial_figure = empty_figure(str(error))
        initial_status = "Waiting for complete CSV rows..."

    app.layout = html.Div(
        [
            html.H1("WIBViz live-replay dashboard"),
            html.Div(initial_status, id="capture-status"),
            dcc.Graph(
                id="event-heatmap",
                figure=initial_figure,
                config={"displaylogo": False, "scrollZoom": True},
            ),
            dcc.Interval(
                id="csv-refresh",
                interval=max(250, int(refresh_seconds * 1000)),
                n_intervals=0,
            ),
        ],
        style={
            "backgroundColor": "#111111",
            "color": "white",
            "padding": "20px",
            "minHeight": "100vh",
        },
    )

    @app.callback(
        Output("event-heatmap", "figure"),
        Output("capture-status", "children"),
        Input("csv-refresh", "n_intervals"),
    )
    def refresh_dashboard(_):
        try:
            stat = csv_path.stat()
            signature = (stat.st_size, stat.st_mtime_ns)
            if signature == cache["signature"]:
                return no_update, no_update

            snapshot = load_snapshot(csv_path, tpc=tpc, plane=plane)
            cache["signature"] = signature
            updated = datetime.fromtimestamp(stat.st_mtime).strftime("%H:%M:%S")
            status = (
                f"Updated {updated} | {snapshot.rows:,} samples | "
                f"{snapshot.wires} wires | ticks "
                f"{snapshot.min_tick}-{snapshot.max_tick}"
            )
            return snapshot.figure, status
        except (OSError, ValueError) as error:
            return no_update, f"Waiting for data: {error}"

    return app


def build_parser():
    parser = argparse.ArgumentParser(
        description="Display a CSV-backed WIBViz heatmap that refreshes as the file grows."
    )
    parser.add_argument("--csv", required=True, help="Graph-ready CSV to monitor.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--tpc", type=int, default=0)
    parser.add_argument("--plane", type=int, default=0)
    parser.add_argument("--refresh-seconds", type=float, default=2.0)
    parser.add_argument(
        "--snapshot-html",
        help="Also write the current interactive figure to this HTML file.",
    )
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="Write --snapshot-html and exit without starting Dash.",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.refresh_seconds <= 0:
        parser.error("--refresh-seconds must be greater than zero")
    if args.html_only and not args.snapshot_html:
        parser.error("--html-only requires --snapshot-html")

    if args.snapshot_html:
        try:
            snapshot = load_snapshot(args.csv, tpc=args.tpc, plane=args.plane)
        except (OSError, ValueError, RuntimeError) as error:
            parser.exit(1, f"wibviz-dashboard: error: {error}\n")
        snapshot.figure.write_html(args.snapshot_html, include_plotlyjs="cdn")
        print(f"Snapshot written to {args.snapshot_html}")
        if args.html_only:
            return 0

    try:
        app = build_app(
            args.csv,
            tpc=args.tpc,
            plane=args.plane,
            refresh_seconds=args.refresh_seconds,
        )
    except RuntimeError as error:
        parser.exit(1, f"wibviz-dashboard: error: {error}\n")

    print(f"Monitoring CSV: {Path(args.csv).resolve()}")
    print(f"Refresh interval: {args.refresh_seconds:g} second(s)")
    app.run(host=args.host, port=args.port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
