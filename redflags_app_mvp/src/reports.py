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


def build_pdf_report(
    flagged_agents: pd.DataFrame,
    summary: pd.DataFrame,
    month_label: str,
    generated_by: str,
) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter, title=f"Reporte Red Flags {month_label}"
    )
    styles = getSampleStyleSheet()

    story = []
    story.append(Paragraph(f"Reporte de Red Flags - {month_label}", styles["Title"]))
    story.append(Spacer(1, 12))
    story.append(
        Paragraph(
            f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Usuario: {generated_by or 'N/D'}",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))

    kpis = [
        [
            "Total agentes",
            str(
                summary["agent_key"].nunique() if "agent_key" in summary.columns else 0
            ),
        ],
        [
            "Agentes con red flags",
            str(
                flagged_agents["agent_key"].nunique() if not flagged_agents.empty else 0
            ),
        ],
        ["Red flags detectadas", str(len(flagged_agents))],
        [
            "Producción total",
            f"{summary['production_monthly_total'].sum():,.2f}"
            if "production_monthly_total" in summary.columns
            else "0.00",
        ],
        [
            "Citas totales",
            f"{summary['appointments_month_total'].sum():,.2f}"
            if "appointments_month_total" in summary.columns
            else "0.00",
        ],
    ]

    kpi_table = Table([["KPI", "Valor"], *kpis], colWidths=[220, 120])
    kpi_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("PADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(kpi_table)
    story.append(Spacer(1, 16))

    story.append(Paragraph("Agentes con red flags", styles["Heading2"]))
    story.append(Spacer(1, 8))

    if flagged_agents.empty:
        story.append(
            Paragraph(
                "No se detectaron red flags para el periodo seleccionado.",
                styles["BodyText"],
            )
        )
    else:
        preview = flagged_agents[
            [
                col
                for col in [
                    "agent_name",
                    "hierarchy",
                    "flag_name",
                    "severity",
                    "reason",
                ]
                if col in flagged_agents.columns
            ]
        ].head(25)
        rows = [list(preview.columns)] + preview.astype(str).values.tolist()
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
        story.append(table)

    doc.build(story)
    buffer.seek(0)
    return buffer.read()
