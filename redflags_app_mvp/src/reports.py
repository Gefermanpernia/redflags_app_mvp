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


def _styled_table(rows):
    table = Table(rows, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("PADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def build_pdf_report(
    flags_df: pd.DataFrame,
    summary: pd.DataFrame,
    final_monitoring_set: pd.DataFrame,
    month_label: str,
    generated_by: str,
) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=f"Reporte Red Flags {month_label}")
    styles = getSampleStyleSheet()

    story = [Paragraph(f"Reporte Ejecutivo de Monitoreo - {month_label}", styles["Title"]), Spacer(1, 10)]
    story.append(Paragraph(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Usuario: {generated_by or 'N/D'}", styles["BodyText"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("1. Executive Summary", styles["Heading2"]))
    kpis = [
        ["Total agentes analizados", str(summary["agent_key"].nunique() if "agent_key" in summary.columns else 0)],
        ["Casos en monitoreo final", str(final_monitoring_set["agent_key"].nunique() if not final_monitoring_set.empty else 0)],
        ["Red flags críticas/semanales", str(len(flags_df[flags_df["flag_id"] != "RF-OBS"]) if not flags_df.empty else 0)],
    ]
    story.append(_styled_table([["KPI", "Valor"], *kpis]))
    story.append(Spacer(1, 12))

    body = final_monitoring_set.copy()
    if body.empty:
        story.append(Paragraph("No hay agentes en el set final de monitoreo.", styles["BodyText"]))
        doc.build(story)
        buffer.seek(0)
        return buffer.read()

    merged = body.merge(
        flags_df.groupby(["month", "agent_key"], as_index=False).agg(
            active_flags=("flag_name", lambda x: ", ".join(sorted(set(x)))),
            explanations=("reason", lambda x: " | ".join(sorted(set(x)))),
        ),
        on=["month", "agent_key"],
        how="left",
    )
    merged["active_flags"] = merged["active_flags"].fillna("")
    merged["explanations"] = merged["explanations"].fillna("")

    def section(title: str, data: pd.DataFrame):
        story.append(Paragraph(title, styles["Heading2"]))
        if data.empty:
            story.append(Paragraph("Sin casos.", styles["BodyText"]))
        else:
            view = data[["agent_name", "hierarchy", "appointments_month_total", "production_monthly_total", "active_flags", "monitoring_source"]].head(30)
            story.append(_styled_table([list(view.columns), *view.astype(str).values.tolist()]))
        story.append(Spacer(1, 10))

    section("2. Critical Red Flags", merged[merged["active_flags"].str.contains("mensual|Sin citas", case=False, na=False)])
    section("3. Weekly Red Flags", merged[merged["active_flags"].str.contains("seman", case=False, na=False)])
    section("4. Observation / Monitoring Cases", merged[merged["active_flags"].str.contains("observ", case=False, na=False)])
    section("5. Manually Included Agents", merged[merged["manual_include"]])
    story.append(Paragraph("6. Conclusion / Operational Recommendation", styles["Heading2"]))
    story.append(Paragraph("Priorizar investigación operativa en casos críticos y seguimiento semanal de casos observacionales.", styles["BodyText"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.read()
