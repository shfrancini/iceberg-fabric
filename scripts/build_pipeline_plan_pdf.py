from __future__ import annotations

import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY_PATH = Path(
    "/Users/scarlettfrancini/Downloads/"
    "visualization_wibwiz_project_and_results_20260803/"
    "capacity_study_20260803_updated/"
    "capacity_summary_data.json"
)
OUTPUT_PATH = REPO_ROOT / "output/pdf/wib_capacity_plan_and_phase_reframe_20260805.pdf"


def load_summary() -> dict:
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Missing evidence summary: {SUMMARY_PATH}")
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


def p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def table(data, col_widths=None, font_size=8.5, header=True) -> Table:
    body_style = ParagraphStyle(
        name=f"TableBody{font_size}",
        fontName="Helvetica",
        fontSize=font_size,
        leading=font_size + 2,
    )
    header_style = ParagraphStyle(
        name=f"TableHeader{font_size}",
        fontName="Helvetica-Bold",
        fontSize=font_size,
        leading=font_size + 2,
        textColor=colors.white,
    )
    wrapped = []
    for row_idx, row in enumerate(data):
        style = header_style if header and row_idx == 0 else body_style
        wrapped.append([Paragraph(str(cell), style) for cell in row])
    t = Table(wrapped, colWidths=col_widths, repeatRows=1 if header else 0)
    style = [
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), font_size),
        ("LEADING", (0, 0), (-1, -1), font_size + 2),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d4d8dc")),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        style += [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0b6f78")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]
    t.setStyle(TableStyle(style))
    return t


def fmt(v, digits=2):
    if v is None:
        return "not measured"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def page_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#6b7280"))
    canvas.drawRightString(
        canvas._pagesize[0] - 0.55 * inch,
        0.35 * inch,
        f"Capacity plan reframe - page {doc.page}",
    )
    canvas.restoreState()


def build_pdf() -> None:
    summary = load_summary()
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="TitleCenter",
            parent=styles["Title"],
            alignment=TA_CENTER,
            fontSize=20,
            leading=24,
            textColor=colors.HexColor("#0f2f33"),
            spaceAfter=12,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Section",
            parent=styles["Heading2"],
            fontSize=13,
            leading=16,
            textColor=colors.HexColor("#0b6f78"),
            spaceBefore=12,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodyTight",
            parent=styles["BodyText"],
            fontSize=9.5,
            leading=12.5,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Small",
            parent=styles["BodyText"],
            fontSize=8,
            leading=10.5,
            textColor=colors.HexColor("#374151"),
        )
    )

    story = []
    story.append(p("WIB/FABRIC Capacity Study: Plan and Phase Reframe", styles["TitleCenter"]))
    story.append(
        p(
            "Purpose: present the current progress without overstating it, then define the next experimental plan aligned with the actual goal: pipeline first, parallelism second.",
            styles["BodyTight"],
        )
    )

    story.append(p("Project Goal", styles["Section"]))
    story.append(
        p(
            "Build a live processing path where each stage hands work to the next stage through a queue, backpressure is explicit, queue depths are measured, and the main KPI is validated output throughput. Parallel decoding should be evaluated only after the single-worker queued pipeline baseline is understood.",
            styles["BodyTight"],
        )
    )

    boundary = summary["boundary"]
    best = summary["best_config_tested"]
    story.append(p("What The Current Evidence Actually Shows", styles["Section"]))
    evidence_rows = [
        ["Evidence item", "Recorded result", "What it can support"],
        [
            "2-worker continuous tests",
            f"Highest sustained passing rate: {boundary['highest_sustained_passing_pps']} PPS; first unsustainable rate: {boundary['first_unsustainable_pps']} PPS.",
            "Supports a preliminary combined result: queued pipeline plus decoder parallelism.",
        ],
        [
            "25 PPS repeat behavior",
            f"Median steady output: {boundary['pass25_steady_validated_packets_s_median']:.2f} packets/s and {boundary['pass25_steady_validated_rows_s_median']:.1f} rows/s.",
            "Supports that the tested 2-worker setup kept up near 25 PPS.",
        ],
        [
            "30 PPS behavior",
            f"Median steady output: {boundary['fail30_steady_validated_packets_s_median']:.2f} packets/s; Q2 filled and blocked upstream.",
            "Supports that 30 PPS was not sustainable in this configuration.",
        ],
        [
            "Best tested configuration",
            f"{best['chunk_packets']}-packet chunks, {best['workers']} decoder workers, queue capacity {best['queue_capacity_chunks_selected']} chunks.",
            "Supports only 'best tested so far', not a global optimum.",
        ],
        [
            "1-worker evidence",
            "At 25 PPS with chunk=128, worker=1, qcap=8, Q2 filled 8/8 and blocked for 192.6 s; trial stopped at gate.",
            "Suggests one worker did not keep up at 25 PPS in that test; it does not define the full 1-worker boundary.",
        ],
    ]
    story.append(table(evidence_rows, col_widths=[1.75 * inch, 3.35 * inch, 4.25 * inch], font_size=8.3))

    story.append(p("Corrected Phase Structure", styles["Section"]))
    phase_rows = [
        ["Phase", "Name", "Status with current evidence"],
        ["0", "Existing baseline / reference behavior", "Available as context, but should be stated carefully by evidence source."],
        ["1", "Pipeline-only implementation and correctness", "Missing as a clean phase. Must use exactly 1 decoder worker."],
        ["2", "Pipeline-only sustained boundary", "Missing. Needed before claiming what pipelining alone achieves."],
        ["3", "Add decoder parallelism", "Partially available. Current 2-worker results belong here, not Phase 1."],
        ["4", "Optimize chunk size, worker count, and queue capacity", "Preliminary only, because it was run before the clean pipeline-only baseline."],
        ["5", "Long endurance validation", "Not run. Proposed 64,000-packet run remains future work."],
    ]
    story.append(table(phase_rows, col_widths=[0.45 * inch, 3.0 * inch, 5.9 * inch], font_size=8.5))

    story.append(p("Presentation-Safe Update", styles["Section"]))
    story.append(
        p(
            "Since the end-of-program presentation, I built and documented an experimental capacity study. It sends incoming work through bounded buffers to two decoder workers, allowing packet intake and decoding to proceed independently while limiting memory growth. In that configuration, the system sustained validated output at 25 PPS and showed buildup and backpressure at 30 PPS. This is useful preliminary evidence, but it does not isolate pipelining alone because decoder parallelism was already enabled.",
            styles["BodyTight"],
        )
    )
    story.append(
        p(
            "The next correction is to run a true pipeline-only baseline with one decoder worker, then compare that result against two or more decoder workers. This will separate the benefit of queues from the benefit of parallel decoding.",
            styles["BodyTight"],
        )
    )

    story.append(p("Next Plan", styles["Section"]))
    plan_rows = [
        ["Step", "Experiment", "Decision rule"],
        ["1", "Run 1-worker queued pipeline correctness on repeated Event 4245 data.", "Must produce exact final validation and queue metrics."],
        ["2", "Sweep 1-worker input rates from low rate upward.", "Find where output no longer keeps up or queues trend upward."],
        ["3", "Repeat the 1-worker boundary.", "Boundary is credible only if pass/fail behavior repeats."],
        ["4", "Run matched 2-worker comparison using same chunk size and queue settings.", "Any gain can be attributed to decoder parallelism only if setup is controlled."],
        ["5", "Then tune chunk size and queue capacity.", "Optimization comes after the pipeline-vs-parallelism comparison."],
        ["6", "Run 64,000-packet endurance on best validated configuration.", "Requires exact validation, stable queue depths, no drops, and enough storage."],
    ]
    story.append(table(plan_rows, col_widths=[0.45 * inch, 4.3 * inch, 4.6 * inch], font_size=8.5))

    story.append(p("Self-Checks Against The Goal", styles["Section"]))
    checks = [
        "Does the plan isolate pipelining before parallelism? Yes: the next required result is one worker only.",
        "Does it measure the right KPI? Yes: validated output throughput, not just replay PPS or capture PPS.",
        "Does it monitor queues? Yes: queue depth, capacity, blocked puts, blocked time, and slope are required.",
        "Does it use enough data? Not yet: the current sustained tests use 6,400 packets; the plan keeps 64,000 packets as the endurance target.",
        "Does it avoid overclaiming? Yes: current 2-worker results are labeled preliminary combined evidence.",
    ]
    for item in checks:
        story.append(p(f"- {item}", styles["BodyTight"]))

    story.append(p("Reasons This Plan Is Bad Or Incomplete", styles["Section"]))
    bad_reasons = [
        "It arrives after some experiments were already run in the wrong order, so the existing phase labels need cleanup.",
        "It delays optimization until after the clean 1-worker baseline, which is scientifically cleaner but slower.",
        "A 1-worker pipeline baseline may perform poorly, making the short-term result look worse even though it is the right control.",
        "64,000 packets may be expensive in time and storage; the saved projection estimates about 15 GB receiver-local evidence and recommends at least 25 GB free.",
        "The plan still depends on the current decoder/export behavior unless a separate decode-vs-serialization measurement is added.",
        "The current best 25 PPS result is far below realistic high-rate detector expectations, so this plan characterizes the prototype rather than proving final live-system readiness.",
    ]
    for item in bad_reasons:
        story.append(p(f"- {item}", styles["BodyTight"]))

    story.append(p("Evidence Source", styles["Section"]))
    story.append(
        p(
            f"Primary local summary used for this PDF: {SUMMARY_PATH}. Generated-at timestamp in the evidence file: {summary['generated_at_utc']}.",
            styles["Small"],
        )
    )

    doc = SimpleDocTemplate(
        str(OUTPUT_PATH),
        pagesize=landscape(letter),
        rightMargin=0.55 * inch,
        leftMargin=0.55 * inch,
        topMargin=0.55 * inch,
        bottomMargin=0.55 * inch,
        title="WIB/FABRIC Capacity Study Plan and Phase Reframe",
        author="Codex",
    )
    doc.build(story, onFirstPage=page_footer, onLaterPages=page_footer)


if __name__ == "__main__":
    build_pdf()
    print(OUTPUT_PATH)
