from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Mapping

import pandas as pd
import streamlit as st

from src.config import (
    DEFAULT_THRESHOLDS,
    REQUIRED_APPOINTMENTS_LONG,
    REQUIRED_APPOINTMENTS_WIDE,
    REQUIRED_PRODUCTION_LONG,
    REQUIRED_PRODUCTION_WIDE,
    ThresholdConfig,
)
from src.parsers import (
    load_excel_sheets,
    load_selected_frames,
    parse_appointments_frames,
    parse_production_frames,
    preview_columns,
)
from src.persistence import load_audit_log, persist_run
from src.pipeline import run_pipeline
from src.reports import build_excel_report, build_pdf_report, dataframe_to_csv_bytes


st.set_page_config(page_title="Red Flags de Agentes", layout="wide")


APP_TITLE = "App de Monitoreo de Red Flags de Agentes"
DEFAULT_MONTH = datetime.now().strftime("%Y-%m")



def render_column_mapping(columns: list[str], field_labels: Mapping[str, str], key_prefix: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    options = [""] + columns
    for field_key, label in field_labels.items():
        mapping[field_key] = st.selectbox(label, options=options, key=f"{key_prefix}_{field_key}")
    return mapping



def validate_mapping(mapping: Mapping[str, str], required_fields: list[str]) -> list[str]:
    missing = [field for field in required_fields if not mapping.get(field)]
    return missing



def render_sheet_preview(frames: Mapping[str, pd.DataFrame], title: str) -> None:
    with st.expander(title, expanded=False):
        for sheet_name, frame in frames.items():
            st.markdown(f"**Hoja:** {sheet_name}")
            st.dataframe(frame.head(5), use_container_width=True)



def build_threshold_config() -> ThresholdConfig:
    with st.sidebar:
        st.header("Configuración")
        generated_by = st.text_input("Usuario que carga", value="operador")
        month_label = st.text_input("Mes de trabajo", value=DEFAULT_MONTH)
        monthly_threshold = st.number_input(
            "Umbral producción mensual sospechosa",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.monthly_production_suspicious),
            step=100.0,
        )
        weekly_threshold = st.number_input(
            "Umbral producción semanal sospechosa",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.weekly_production_suspicious),
            step=100.0,
        )
        spike_threshold = st.number_input(
            "Umbral pico última semana",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.spike_last_week_threshold),
            step=100.0,
        )
        few_appts = st.number_input(
            "Definición de pocas citas",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.few_appointments_threshold),
            step=1.0,
        )
        insignificant_prod = st.number_input(
            "Definición de producción insignificante",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.insignificant_production_threshold),
            step=50.0,
        )
        use_open_week_partial = st.checkbox(
            "Usar semana actual abierta como MTD parcial",
            value=DEFAULT_THRESHOLDS.use_open_week_partial,
        )

    st.session_state["generated_by"] = generated_by
    st.session_state["month_label"] = month_label
    return ThresholdConfig(
        monthly_production_suspicious=monthly_threshold,
        weekly_production_suspicious=weekly_threshold,
        spike_last_week_threshold=spike_threshold,
        few_appointments_threshold=few_appts,
        insignificant_production_threshold=insignificant_prod,
        use_open_week_partial=use_open_week_partial,
    )



def render_upload_and_process(config: ThresholdConfig) -> None:
    st.subheader("Carga de archivos")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Producción")
        production_file = st.file_uploader("Sube Excel de producción", type=["xlsx", "xlsm", "xls"], key="production_file")
        production_layout = st.radio(
            "Formato del Excel de producción",
            options=["long", "wide"],
            format_func=lambda value: "Long (una fila por semana)" if value == "long" else "Wide (columnas por semana)",
            key="production_layout",
        )

        production_sheets = []
        production_frames = {}
        production_mapping: Dict[str, str] = {}
        if production_file is not None:
            available_sheets = load_excel_sheets(production_file)
            production_sheets = st.multiselect(
                "Hojas a procesar",
                options=available_sheets,
                default=available_sheets[:1],
                key="production_sheets",
            )
            if production_sheets:
                production_frames = load_selected_frames(production_file, production_sheets)
                render_sheet_preview(production_frames, "Preview hojas de producción")
                production_columns = preview_columns(production_frames)
                production_mapping = render_column_mapping(
                    production_columns,
                    REQUIRED_PRODUCTION_LONG if production_layout == "long" else REQUIRED_PRODUCTION_WIDE,
                    key_prefix="production_map",
                )

    with col2:
        st.markdown("### Citas")
        appointments_file = st.file_uploader("Sube Excel de citas", type=["xlsx", "xlsm", "xls"], key="appointments_file")
        appointments_layout = st.radio(
            "Formato del Excel de citas",
            options=["long", "wide"],
            format_func=lambda value: "Long (una fila por semana)" if value == "long" else "Wide (columnas por semana)",
            key="appointments_layout",
        )

        appointments_sheets = []
        appointments_frames = {}
        appointments_mapping: Dict[str, str] = {}
        if appointments_file is not None:
            available_sheets = load_excel_sheets(appointments_file)
            appointments_sheets = st.multiselect(
                "Hojas a procesar",
                options=available_sheets,
                default=available_sheets[:1],
                key="appointments_sheets",
            )
            if appointments_sheets:
                appointments_frames = load_selected_frames(appointments_file, appointments_sheets)
                render_sheet_preview(appointments_frames, "Preview hojas de citas")
                appointments_columns = preview_columns(appointments_frames)
                appointments_mapping = render_column_mapping(
                    appointments_columns,
                    REQUIRED_APPOINTMENTS_LONG if appointments_layout == "long" else REQUIRED_APPOINTMENTS_WIDE,
                    key_prefix="appointments_map",
                )

    required_prod = ["agent_name", "week", "production_mtd"] if st.session_state.get("production_layout") == "long" else ["agent_name", "mtd_week_1"]
    required_appt = ["agent_name", "week", "appointments"] if st.session_state.get("appointments_layout") == "long" else ["agent_name", "appointments_week_1"]

    if st.button("Procesar archivos", type="primary"):
        if production_file is None or appointments_file is None:
            st.error("Debes subir ambos archivos: producción y citas.")
            return
        if not production_sheets or not appointments_sheets:
            st.error("Selecciona al menos una hoja en ambos archivos.")
            return

        missing_prod = validate_mapping(production_mapping, required_prod)
        missing_appt = validate_mapping(appointments_mapping, required_appt)
        if missing_prod or missing_appt:
            messages = []
            if missing_prod:
                messages.append(f"Producción: faltan {', '.join(missing_prod)}")
            if missing_appt:
                messages.append(f"Citas: faltan {', '.join(missing_appt)}")
            st.error(" | ".join(messages))
            return

        raw_production = parse_production_frames(
            production_frames,
            production_mapping,
            layout=production_layout,
            fallback_month=st.session_state["month_label"],
        )
        raw_appointments = parse_appointments_frames(
            appointments_frames,
            appointments_mapping,
            layout=appointments_layout,
            fallback_month=st.session_state["month_label"],
        )

        if raw_production.empty:
            st.error("No se pudo extraer producción. Revisa el mapeo o el formato de las hojas.")
            return
        if raw_appointments.empty:
            st.error("No se pudo extraer citas. Revisa el mapeo o el formato de las hojas.")
            return

        results = run_pipeline(raw_production, raw_appointments, config)
        persist_run(
            month_label=st.session_state["month_label"],
            generated_by=st.session_state["generated_by"],
            raw_production=raw_production,
            raw_appointments=raw_appointments,
            weekly_df=results["weekly"],
            monthly_df=results["monthly"],
            flags_df=results["flags"],
            summary_df=results["summary"],
        )
        st.session_state["results"] = results
        st.session_state["raw_production"] = raw_production
        st.session_state["raw_appointments"] = raw_appointments
        st.success("Procesamiento completado. Ya puedes revisar dashboard, detalle y reportes.")



def render_dashboard() -> None:
    st.subheader("Dashboard")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para ver resultados.")
        return

    weekly = results["weekly"]
    flags = results["flags"]
    summary = results["summary"]
    flagged = results["flagged_agents"]

    total_agents = summary["agent_key"].nunique() if not summary.empty else 0
    flagged_agents = flagged["agent_key"].nunique() if not flagged.empty else 0
    total_flags = len(flags)
    total_production = float(summary["production_monthly_total"].sum()) if not summary.empty else 0.0
    total_appointments = float(summary["appointments_month_total"].sum()) if not summary.empty else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Agentes analizados", f"{total_agents}")
    k2.metric("Agentes con red flags", f"{flagged_agents}")
    k3.metric("Red flags", f"{total_flags}")
    k4.metric("Producción total", f"{total_production:,.2f}")
    k5.metric("Citas totales", f"{total_appointments:,.0f}")

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.markdown("#### Red flags por tipo")
        if flags.empty:
            st.info("Sin red flags detectadas.")
        else:
            type_counts = flags.groupby("flag_name").size().rename("count")
            st.bar_chart(type_counts)

    with chart_col2:
        st.markdown("#### Red flags por semana")
        if flags.empty:
            st.info("Sin red flags detectadas.")
        else:
            week_counts = flags[flags["week"].notna()].groupby("week").size().rename("count")
            if week_counts.empty:
                st.info("No hay red flags semanales.")
            else:
                st.bar_chart(week_counts)

    st.markdown("#### Tabla de agentes con red flags")
    if flagged.empty:
        st.success("No hay agentes sospechosos con las reglas actuales.")
    else:
        st.dataframe(flagged, use_container_width=True)

    with st.expander("Datos semanales consolidados"):
        st.dataframe(weekly, use_container_width=True)



def render_agent_detail() -> None:
    st.subheader("Drill-down por agente")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para habilitar el detalle por agente.")
        return

    weekly = results["weekly"]
    flags = results["flags"]
    if weekly.empty:
        st.info("No hay datos semanales disponibles.")
        return

    agents = sorted(weekly["agent_name"].dropna().unique().tolist())
    agent_name = st.selectbox("Selecciona un agente", options=agents)
    agent_data = weekly[weekly["agent_name"] == agent_name].copy().sort_values(["month", "week"])
    agent_flags = flags[flags["agent_name"] == agent_name].copy()

    top_left, top_right = st.columns(2)
    with top_left:
        st.markdown("#### Evolución semanal de citas")
        appointments_view = agent_data[["week", "appointments"]].set_index("week")
        st.line_chart(appointments_view)

    with top_right:
        st.markdown("#### Evolución semanal de producción")
        production_view = agent_data[["week", "production_weekly_closed", "production_weekly_effective"]].set_index("week")
        st.line_chart(production_view)

    st.markdown("#### Producción MTD vs semanal cerrada")
    comparison = agent_data[["week", "production_mtd", "production_weekly_closed", "production_weekly_effective"]].set_index("week")
    st.dataframe(comparison, use_container_width=True)

    st.markdown("#### Histórico de red flags")
    if agent_flags.empty:
        st.info("Este agente no tiene red flags activas en el periodo procesado.")
    else:
        st.dataframe(agent_flags, use_container_width=True)

    st.markdown("#### Traza completa del agente")
    st.dataframe(agent_data, use_container_width=True)



def render_reports() -> None:
    st.subheader("Reportes y exportables")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para habilitar reportes.")
        return

    flags = results["flags"]
    flagged = results["flagged_agents"]
    weekly = results["weekly"]
    monthly = results["monthly"]
    summary = results["summary"]
    month_label = st.session_state.get("month_label", DEFAULT_MONTH)
    generated_by = st.session_state.get("generated_by", "operador")

    excel_bytes = build_excel_report(
        {
            "flagged_agents": flagged,
            "flags": flags,
            "weekly": weekly,
            "monthly": monthly,
            "summary": summary,
        }
    )
    csv_bytes = dataframe_to_csv_bytes(flagged if not flagged.empty else summary)
    pdf_bytes = build_pdf_report(flags, summary, month_label=month_label, generated_by=generated_by)

    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button(
            label="Descargar reporte CSV",
            data=csv_bytes,
            file_name=f"red_flags_{month_label}.csv",
            mime="text/csv",
        )
    with d2:
        st.download_button(
            label="Descargar reporte Excel",
            data=excel_bytes,
            file_name=f"red_flags_{month_label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with d3:
        st.download_button(
            label="Descargar reporte PDF",
            data=pdf_bytes,
            file_name=f"red_flags_{month_label}.pdf",
            mime="application/pdf",
        )

    st.markdown("#### Vista previa del reporte")
    st.dataframe(flagged if not flagged.empty else summary, use_container_width=True)



def render_history() -> None:
    st.subheader("Histórico y auditoría")
    audit_log = load_audit_log()
    if audit_log.empty:
        st.info("Todavía no hay corridas persistidas en data/history.")
        return
    st.dataframe(audit_log.sort_values("timestamp", ascending=False), use_container_width=True)



def render_notes() -> None:
    with st.expander("Notas del MVP", expanded=False):
        st.markdown(
            """
            - Soporta archivos **long** y **wide** para producción y citas.
            - Convierte producción **MTD** a producción semanal cerrada.
            - Permite una **semana abierta** donde la producción efectiva usa el MTD actual como parcial.
            - Las reglas iniciales implementadas son RF-001, RF-002 y RF-003.
            - Guarda trazabilidad local en `data/history` y `data/audit_log.csv`.
            - El mapeo de columnas se hace en UI para tolerar cambios en el orden o nombre de columnas.
            """
        )



def main() -> None:
    st.title(APP_TITLE)
    st.caption(
        "MVP para cruzar producción vs citas, detectar agentes sospechosos y exportar reportes operativos."
    )
    render_notes()

    config = build_threshold_config()

    tabs = st.tabs(["Carga", "Dashboard", "Detalle", "Reportes", "Histórico"])
    with tabs[0]:
        render_upload_and_process(config)
    with tabs[1]:
        render_dashboard()
    with tabs[2]:
        render_agent_detail()
    with tabs[3]:
        render_reports()
    with tabs[4]:
        render_history()


if __name__ == "__main__":
    main()
