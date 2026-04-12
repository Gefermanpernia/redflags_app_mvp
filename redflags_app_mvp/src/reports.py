from __future__ import annotations

import io
from datetime import datetime
from typing import Dict

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def build_excel_report(sheets: Dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        for sheet_name, df in sheets.items():
            safe_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)
            worksheet = writer.sheets[safe_name]
            for idx, col in enumerate(df.columns):
                width = min(max(len(str(col)), 14), 40)
                worksheet.set_column(idx, idx, width)
    buffer.seek(0)
    return buffer.read()


def _section_table(title: str, frame: pd.DataFrame, story: list, styles) -> None:
    story.append(Paragraph(title, styles["Heading2"]))
    story.append(Spacer(1, 6))
    if frame.empty:
        story.append(Paragraph("Sin casos.", styles["BodyText"]))
        story.append(Spacer(1, 8))
        return
    cols = [c for c in ["agent_name", "hierarchy", "appointments_month_total", "production_monthly_total", "active_flags", "inclusion_reason"] if c in frame.columns]
    rows = [cols] + frame[cols].astype(str).values.tolist()
    table = Table(rows, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("PADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)
    story.append(Spacer(1, 10))


def build_pdf_report(final_monitoring: pd.DataFrame, flags: pd.DataFrame, month_label: str, generated_by: str, unified_operational: pd.DataFrame | None = None) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=f"Reporte Red Flags {month_label}")
    styles = getSampleStyleSheet()
    story = [Paragraph(f"Reporte Ejecutivo de Monitoreo - {month_label}", styles["Title"]), Spacer(1, 10)]
    story.append(Paragraph(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Usuario: {generated_by or 'N/D'}", styles["BodyText"]))
    story.append(Spacer(1, 12))

    story.append(Paragraph("1. Executive Summary", styles["Heading2"]))
    story.append(Paragraph(f"Casos finales en monitoreo: {len(final_monitoring)}. Flags totales detectadas: {len(flags)}.", styles["BodyText"]))
    story.append(Spacer(1, 10))

    critical_keys = set(flags[flags["flag_id"].isin(["RF-001", "RF-002"])]["agent_key"].tolist()) if not flags.empty else set()
    weekly_keys = set(flags[flags["flag_id"] == "RF-003"]["agent_key"].tolist()) if not flags.empty else set()
    obs_keys = set(flags[flags["flag_id"].astype(str).str.startswith("OBS")]["agent_key"].tolist()) if not flags.empty else set()

    _section_table("2. Critical Red Flags", final_monitoring[final_monitoring["agent_key"].isin(critical_keys)], story, styles)
    _section_table("3. Weekly Red Flags", final_monitoring[final_monitoring["agent_key"].isin(weekly_keys)], story, styles)
    _section_table("4. Observation / Monitoring Cases", final_monitoring[final_monitoring["agent_key"].isin(obs_keys)], story, styles)
    _section_table("5. Manually Included Agents", final_monitoring[final_monitoring["manual_include"]], story, styles)
    story.append(Paragraph("6. Dataset operativo unificado", styles["Heading2"]))
    if unified_operational is None or unified_operational.empty:
        story.append(Paragraph("Sin registros operativos unificados en el período.", styles["BodyText"]))
    else:
        ops = unified_operational.groupby(["record_type", "source_origin"], as_index=False).agg(registros=("id", "count"), monto_total=("amount", "sum"))
        rows = [["record_type", "source_origin", "registros", "monto_total"]] + ops.astype(str).values.tolist()
        ops_table = Table(rows, repeatRows=1)
        ops_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f766e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("PADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(ops_table)
    story.append(Spacer(1, 10))
    story.append(Paragraph("7. Conclusion / Operational Recommendation", styles["Heading2"]))
    story.append(Paragraph("Priorizar seguimiento en casos críticos y reforzar control de citas en agentes observados.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.read()
