from __future__ import annotations

from datetime import datetime
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
from src.data_quality import (
    build_quality_summary,
    detect_mixed_months,
    validate_sheet_columns,
)
from src.normalization import load_alias_mapping
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


def render_column_mapping(
    columns: list[str], field_labels: Mapping[str, str], key_prefix: str
) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    options = [""] + columns
    for field_key, label in field_labels.items():
        mapping[field_key] = st.selectbox(
            label, options=options, key=f"{key_prefix}_{field_key}"
        )
    return mapping


def validate_mapping(
    mapping: Mapping[str, str], required_fields: list[str]
) -> list[str]:
    return [field for field in required_fields if not mapping.get(field)]


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
        alias_file = st.text_input("CSV de alias (opcional)", value="")
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
    st.session_state["alias_file"] = alias_file
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
        production_file = st.file_uploader(
            "Sube Excel de producción",
            type=["xlsx", "xlsm", "xls"],
            key="production_file",
        )
        production_layout = st.radio(
            "Formato del Excel de producción",
            options=["long", "wide"],
            format_func=lambda value: "Long (una fila por semana)"
            if value == "long"
            else "Wide (columnas por semana)",
            key="production_layout",
        )
        production_sheets, production_frames, production_mapping = [], {}, {}
        if production_file is not None:
            production_sheets = st.multiselect(
                "Hojas a procesar",
                options=load_excel_sheets(production_file),
                key="production_sheets",
            )
            if production_sheets:
                production_frames = load_selected_frames(
                    production_file, production_sheets
                )
                render_sheet_preview(production_frames, "Preview hojas de producción")
                production_mapping = render_column_mapping(
                    preview_columns(production_frames),
                    REQUIRED_PRODUCTION_LONG
                    if production_layout == "long"
                    else REQUIRED_PRODUCTION_WIDE,
                    key_prefix="production_map",
                )

    with col2:
        st.markdown("### Citas")
        appointments_file = st.file_uploader(
            "Sube Excel de citas", type=["xlsx", "xlsm", "xls"], key="appointments_file"
        )
        appointments_layout = st.radio(
            "Formato del Excel de citas",
            options=["long", "wide"],
            format_func=lambda value: "Long (una fila por semana)"
            if value == "long"
            else "Wide (columnas por semana)",
            key="appointments_layout",
        )
        appointments_sheets, appointments_frames, appointments_mapping = [], {}, {}
        if appointments_file is not None:
            appointments_sheets = st.multiselect(
                "Hojas a procesar",
                options=load_excel_sheets(appointments_file),
                key="appointments_sheets",
            )
            if appointments_sheets:
                appointments_frames = load_selected_frames(
                    appointments_file, appointments_sheets
                )
                render_sheet_preview(appointments_frames, "Preview hojas de citas")
                appointments_mapping = render_column_mapping(
                    preview_columns(appointments_frames),
                    REQUIRED_APPOINTMENTS_LONG
                    if appointments_layout == "long"
                    else REQUIRED_APPOINTMENTS_WIDE,
                    key_prefix="appointments_map",
                )

    required_prod = (
        ["agent_name", "week", "production_mtd"]
        if production_layout == "long"
        else ["agent_name", "mtd_week_1"]
    )
    required_appt = (
        ["agent_name", "week", "appointments"]
        if appointments_layout == "long"
        else ["agent_name", "appointments_week_1"]
    )

    if st.button("Procesar archivos", type="primary"):
        if production_file is None or appointments_file is None:
            st.error("Debes subir ambos archivos: producción y citas.")
            return

        errors = []
        errors += (
            [
                f"Producción: faltan {', '.join(validate_mapping(production_mapping, required_prod))}"
            ]
            if validate_mapping(production_mapping, required_prod)
            else []
        )
        errors += (
            [
                f"Citas: faltan {', '.join(validate_mapping(appointments_mapping, required_appt))}"
            ]
            if validate_mapping(appointments_mapping, required_appt)
            else []
        )
        errors += validate_sheet_columns(
            production_frames, production_mapping, required_prod, "Producción"
        )
        errors += validate_sheet_columns(
            appointments_frames, appointments_mapping, required_appt, "Citas"
        )
        if errors:
            for err in errors:
                st.error(err)
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

        mixed_month_errors = detect_mixed_months(
            raw_production, "Producción"
        ) + detect_mixed_months(raw_appointments, "Citas")
        if mixed_month_errors:
            for err in mixed_month_errors:
                st.error(err)
            st.error(
                "Se bloqueó el procesamiento por calidad de datos (meses mezclados)."
            )
            return

        alias_mapping = load_alias_mapping(st.session_state.get("alias_file"))
        results = run_pipeline(
            raw_production, raw_appointments, config, alias_mapping=alias_mapping
        )
        run_id = persist_run(
            month_label=st.session_state["month_label"],
            generated_by=st.session_state["generated_by"],
            production_file_name=getattr(production_file, "name", "production.xlsx"),
            appointments_file_name=getattr(
                appointments_file, "name", "appointments.xlsx"
            ),
            raw_production=raw_production,
            raw_appointments=raw_appointments,
            weekly_df=results["weekly"],
            monthly_df=results["monthly"],
            flags_df=results["flags"],
            summary_df=results["summary"],
        )
        st.session_state["results"] = results
        st.session_state["quality_summary"] = build_quality_summary(
            raw_production, raw_appointments
        )
        st.success(f"Procesamiento completado. Run ID: {run_id}.")


def render_dashboard() -> None:
    st.subheader("Dashboard")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para ver resultados.")
        return

    flags = results["flags"].copy()
    summary = results["summary"].copy()

    months = ["Todos"] + sorted(summary["month"].astype(str).unique().tolist())
    month_filter = st.selectbox("Mes", months)
    week_filter = st.multiselect(
        "Semana",
        options=sorted([int(v) for v in flags["week"].dropna().unique().tolist()]),
    )
    hierarchy_filter = st.multiselect(
        "Jerarquía",
        options=sorted(summary["hierarchy"].dropna().astype(str).unique().tolist()),
    )
    severity_filter = st.multiselect(
        "Severidad",
        options=sorted(flags["severity"].dropna().astype(str).unique().tolist()),
    )
    flag_type_filter = st.multiselect(
        "Tipo bandera",
        options=sorted(flags["flag_id"].dropna().astype(str).unique().tolist()),
    )

    if month_filter != "Todos":
        flags = flags[flags["month"] == month_filter]
        summary = summary[summary["month"] == month_filter]
    if week_filter:
        flags = flags[flags["week"].isin(week_filter)]
    if hierarchy_filter:
        flags = flags[flags["hierarchy"].isin(hierarchy_filter)]
        summary = summary[summary["hierarchy"].isin(hierarchy_filter)]
    if severity_filter:
        flags = flags[flags["severity"].isin(severity_filter)]
    if flag_type_filter:
        flags = flags[flags["flag_id"].isin(flag_type_filter)]

    flagged_keys = set(flags["agent_key"].tolist())
    flagged = summary[summary["agent_key"].isin(flagged_keys)].copy()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Agentes analizados",
        f"{summary['agent_key'].nunique() if not summary.empty else 0}",
    )
    k2.metric("Agentes con red flags", f"{len(flagged_keys)}")
    k3.metric("Red flags", f"{len(flags)}")
    k4.metric(
        "Riesgo promedio",
        f"{flags['risk_score'].mean():.1f}" if not flags.empty else "0.0",
    )

    st.markdown("#### KPIs por jerarquía")
    if not summary.empty:
        kpi_h = summary.groupby("hierarchy", as_index=False).agg(
            agentes=("agent_key", "nunique"),
            produccion=("production_monthly_total", "sum"),
            citas=("appointments_month_total", "sum"),
            riesgo_max=("risk_score", "max"),
        )
        st.dataframe(
            kpi_h.sort_values(["riesgo_max", "produccion"], ascending=[False, False]),
            use_container_width=True,
        )

    st.markdown("#### Tabla de agentes sospechosos")
    if flagged.empty:
        st.success("No hay agentes sospechosos con los filtros actuales.")
    else:
        st.dataframe(
            flagged.sort_values(
                ["risk_score", "severity", "production_monthly_total"],
                ascending=[False, False, False],
            ),
            use_container_width=True,
        )

    quality = st.session_state.get("quality_summary")
    if quality is not None:
        st.markdown("#### Resumen de calidad de datos")
        st.dataframe(quality, use_container_width=True)


def render_agent_detail() -> None:
    st.subheader("Drill-down por agente")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para habilitar el detalle por agente.")
        return
    weekly = results["weekly"]
    flags = results["flags"]
    if weekly.empty:
        return
    agent_name = st.selectbox(
        "Selecciona un agente",
        options=sorted(weekly["agent_name"].dropna().unique().tolist()),
    )
    st.dataframe(
        weekly[weekly["agent_name"] == agent_name].sort_values(["month", "week"]),
        use_container_width=True,
    )
    st.dataframe(flags[flags["agent_name"] == agent_name], use_container_width=True)


def render_reports() -> None:
    st.subheader("Reportes y exportables")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para habilitar reportes.")
        return
    month_label = st.session_state.get("month_label", DEFAULT_MONTH)
    generated_by = st.session_state.get("generated_by", "operador")
    excel_bytes = build_excel_report(results)
    csv_bytes = dataframe_to_csv_bytes(results["summary"])
    pdf_bytes = build_pdf_report(
        results["flags"],
        results["summary"],
        month_label=month_label,
        generated_by=generated_by,
    )
    c1, c2, c3 = st.columns(3)
    c1.download_button(
        "Descargar CSV",
        data=csv_bytes,
        file_name=f"red_flags_{month_label}.csv",
        mime="text/csv",
    )
    c2.download_button(
        "Descargar Excel",
        data=excel_bytes,
        file_name=f"red_flags_{month_label}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    c3.download_button(
        "Descargar PDF",
        data=pdf_bytes,
        file_name=f"red_flags_{month_label}.pdf",
        mime="application/pdf",
    )


def render_history() -> None:
    st.subheader("Histórico y auditoría")
    st.dataframe(load_audit_log(), use_container_width=True)


def main() -> None:
    st.title(APP_TITLE)
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
