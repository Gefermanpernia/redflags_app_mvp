from __future__ import annotations

from datetime import datetime
from typing import Dict, Mapping

import pandas as pd
import streamlit as st

from src.config import (
    DEFAULT_THRESHOLDS,
    LEGACY_APPOINTMENTS_SHEETS,
    LEGACY_PRODUCTION_SHEETS,
    REQUIRED_APPOINTMENTS_LONG,
    REQUIRED_APPOINTMENTS_WIDE,
    REQUIRED_PRODUCTION_LONG,
    REQUIRED_PRODUCTION_WIDE,
    ThresholdConfig,
)
from src.data_quality import build_quality_summary, detect_mixed_months, validate_sheet_columns
from src.normalization import load_alias_mapping
from src.parsers import load_excel_sheets, load_selected_frames, parse_appointments_frames, parse_production_frames, preview_columns
from src.persistence import load_audit_log, load_overrides, persist_override, persist_run
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
    return [field for field in required_fields if not mapping.get(field)]


def suggest_legacy_sheets(all_sheets: list[str], legacy_names: list[str]) -> list[str]:
    normalized = {s.lower().strip(): s for s in all_sheets}
    picked = []
    for name in legacy_names:
        if name in normalized:
            picked.append(normalized[name])
    return picked


def build_threshold_config() -> ThresholdConfig:
    with st.sidebar:
        st.header("Configuración")
        generated_by = st.text_input("Usuario que carga", value="operador")
        month_label = st.text_input("Mes de trabajo", value=DEFAULT_MONTH)
        alias_file = st.text_input("CSV de alias (opcional)", value="")
        snapshot_date = st.date_input("Snapshot fecha producción", value=datetime.now().date())
        monthly_threshold = st.number_input("Umbral producción mensual sospechosa", min_value=0.0, value=float(DEFAULT_THRESHOLDS.monthly_production_suspicious), step=100.0)
        weekly_threshold = st.number_input("Umbral producción semanal sospechosa", min_value=0.0, value=float(DEFAULT_THRESHOLDS.weekly_production_suspicious), step=100.0)
        weekly_strict = st.number_input("Umbral semanal estricto", min_value=0.0, value=float(DEFAULT_THRESHOLDS.weekly_production_strict), step=100.0)
        weekly_obs = st.number_input("Piso observación semanal", min_value=0.0, value=float(DEFAULT_THRESHOLDS.weekly_observation_floor), step=100.0)
        spike_threshold = st.number_input("Umbral pico última semana", min_value=0.0, value=float(DEFAULT_THRESHOLDS.spike_last_week_threshold), step=100.0)
        few_appts = st.number_input("Definición de pocas citas", min_value=0.0, value=float(DEFAULT_THRESHOLDS.few_appointments_threshold), step=1.0)
        insignificant_prod = st.number_input("Definición de producción insignificante", min_value=0.0, value=float(DEFAULT_THRESHOLDS.insignificant_production_threshold), step=50.0)
        use_open_week_partial = st.checkbox("Usar semana actual abierta como MTD parcial", value=DEFAULT_THRESHOLDS.use_open_week_partial)
        include_open_week_as_completed = st.checkbox("Tratar semana abierta como completada", value=DEFAULT_THRESHOLDS.include_open_week_as_completed)

    st.session_state.update({"generated_by": generated_by, "month_label": month_label, "alias_file": alias_file, "snapshot_date": snapshot_date.strftime("%Y-%m-%d")})
    return ThresholdConfig(
        monthly_production_suspicious=monthly_threshold,
        weekly_production_suspicious=weekly_threshold,
        weekly_production_strict=weekly_strict,
        weekly_observation_floor=weekly_obs,
        spike_last_week_threshold=spike_threshold,
        few_appointments_threshold=few_appts,
        insignificant_production_threshold=insignificant_prod,
        use_open_week_partial=use_open_week_partial,
        include_open_week_as_completed=include_open_week_as_completed,
    )


def render_upload_and_process(config: ThresholdConfig) -> None:
    st.subheader("Carga de archivos")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Producción")
        production_file = st.file_uploader("Sube Excel de producción", type=["xlsx", "xlsm", "xls"], key="production_file")
        production_layout = st.radio("Formato del Excel de producción", options=["long", "wide"], key="production_layout")
        production_sheets, production_frames, production_mapping = [], {}, {}
        if production_file is not None:
            all_sheets = load_excel_sheets(production_file)
            production_sheets = st.multiselect("Hojas a procesar", options=all_sheets, default=suggest_legacy_sheets(all_sheets, LEGACY_PRODUCTION_SHEETS), key="production_sheets")
            if production_sheets:
                production_frames = load_selected_frames(production_file, production_sheets)
                production_mapping = render_column_mapping(preview_columns(production_frames), REQUIRED_PRODUCTION_LONG if production_layout == "long" else REQUIRED_PRODUCTION_WIDE, key_prefix="production_map")

    with col2:
        st.markdown("### Citas")
        appointments_file = st.file_uploader("Sube Excel de citas", type=["xlsx", "xlsm", "xls"], key="appointments_file")
        appointments_layout = st.radio("Formato del Excel de citas", options=["long", "wide"], key="appointments_layout")
        appointments_sheets, appointments_frames, appointments_mapping = [], {}, {}
        if appointments_file is not None:
            all_sheets = load_excel_sheets(appointments_file)
            appointments_sheets = st.multiselect("Hojas a procesar", options=all_sheets, default=suggest_legacy_sheets(all_sheets, LEGACY_APPOINTMENTS_SHEETS), key="appointments_sheets")
            if appointments_sheets:
                appointments_frames = load_selected_frames(appointments_file, appointments_sheets)
                appointments_mapping = render_column_mapping(preview_columns(appointments_frames), REQUIRED_APPOINTMENTS_LONG if appointments_layout == "long" else REQUIRED_APPOINTMENTS_WIDE, key_prefix="appointments_map")

    if st.button("Procesar archivos", type="primary"):
        if production_file is None or appointments_file is None:
            st.error("Debes subir ambos archivos: producción y citas.")
            return
        required_prod = ["agent_name", "week", "production_mtd"] if production_layout == "long" else ["agent_name", "mtd_week_1"]
        required_appt = ["agent_name", "week", "appointments"] if appointments_layout == "long" else ["agent_name", "appointments_week_1"]
        errors = []
        errors += [f"Producción: faltan {', '.join(validate_mapping(production_mapping, required_prod))}"] if validate_mapping(production_mapping, required_prod) else []
        errors += [f"Citas: faltan {', '.join(validate_mapping(appointments_mapping, required_appt))}"] if validate_mapping(appointments_mapping, required_appt) else []
        errors += validate_sheet_columns(production_frames, production_mapping, required_prod, "Producción")
        errors += validate_sheet_columns(appointments_frames, appointments_mapping, required_appt, "Citas")
        if errors:
            for err in errors:
                st.error(err)
            return

        alias_mapping = load_alias_mapping(st.session_state.get("alias_file"))
        raw_production, prod_conflicts = parse_production_frames(production_frames, production_mapping, layout=production_layout, fallback_month=st.session_state["month_label"], fallback_snapshot_date=st.session_state["snapshot_date"], alias_mapping=alias_mapping)
        raw_appointments, appt_conflicts = parse_appointments_frames(appointments_frames, appointments_mapping, layout=appointments_layout, fallback_month=st.session_state["month_label"], alias_mapping=alias_mapping)
        conflicts = pd.concat([prod_conflicts, appt_conflicts], ignore_index=True) if not prod_conflicts.empty or not appt_conflicts.empty else pd.DataFrame()

        mixed_month_errors = detect_mixed_months(raw_production, "Producción") + detect_mixed_months(raw_appointments, "Citas")
        if mixed_month_errors:
            for err in mixed_month_errors:
                st.error(err)
            return

        overrides = load_overrides(st.session_state["month_label"])
        results = run_pipeline(raw_production, raw_appointments, config, alias_mapping=alias_mapping, monitoring_overrides=overrides, month_label=st.session_state["month_label"])
        run_id = persist_run(
            month_label=st.session_state["month_label"],
            generated_by=st.session_state["generated_by"],
            production_file_name=getattr(production_file, "name", "production.xlsx"),
            appointments_file_name=getattr(appointments_file, "name", "appointments.xlsx"),
            raw_production=raw_production,
            raw_appointments=raw_appointments,
            weekly_df=results["weekly"],
            monthly_df=results["monthly"],
            flags_df=results["flags"],
            summary_df=results["summary"],
            conflicts_df=conflicts,
        )
        st.session_state.update({"results": results, "quality_summary": build_quality_summary(raw_production, raw_appointments), "import_conflicts": conflicts})
        st.success(f"Procesamiento completado. Run ID: {run_id}.")


def render_dashboard() -> None:
    st.subheader("Dashboard")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para ver resultados.")
        return

    flags = results["flags"].copy()
    summary = results["summary"].copy()

    st.markdown("#### Tabla de monitoreo (una fila por agente canónico)")
    st.dataframe(summary, use_container_width=True)

    st.markdown("#### Controles manuales de inclusión/exclusión")
    with st.form("override_form"):
        agent_key = st.selectbox("Agente", options=summary["agent_key"].unique().tolist())
        action_type = st.radio("Acción", options=["include", "exclude"])
        reason = st.text_area("Razón", value="")
        submitted = st.form_submit_button("Guardar override")
        if submitted:
            persist_override(agent_key, st.session_state["month_label"], action_type, reason, st.session_state["generated_by"])
            st.success("Override guardado. Reprocesa para reflejarlo.")

    conflicts = st.session_state.get("import_conflicts")
    if conflicts is not None:
        st.markdown("#### Validación de importación")
        st.dataframe(conflicts, use_container_width=True)


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
    agent_key = st.selectbox("Selecciona un agente", options=sorted(weekly["agent_key"].dropna().unique().tolist()))
    st.dataframe(weekly[weekly["agent_key"] == agent_key].sort_values(["month", "week"]), use_container_width=True)
    st.dataframe(flags[flags["agent_key"] == agent_key], use_container_width=True)


def render_reports() -> None:
    st.subheader("Reportes y exportables")
    results = st.session_state.get("results")
    if not results:
        st.info("Procesa archivos para habilitar reportes.")
        return
    month_label = st.session_state.get("month_label", DEFAULT_MONTH)
    generated_by = st.session_state.get("generated_by", "operador")

    final_set = results["summary"][results["summary"]["in_final_monitoring_set"]].copy()
    st.markdown("#### Selección final antes de PDF")
    selectable = final_set[["agent_key", "agent_name", "monitoring_source", "manual_exclude"]].copy()
    if not selectable.empty:
        selectable["include_in_pdf"] = ~selectable["manual_exclude"]
        edited = st.data_editor(selectable, use_container_width=True)
        chosen = edited[edited["include_in_pdf"]]["agent_key"].tolist()
        final_set = final_set[final_set["agent_key"].isin(chosen)]

    excel_bytes = build_excel_report(results)
    csv_bytes = dataframe_to_csv_bytes(results["summary"])
    pdf_bytes = build_pdf_report(results["flags"], results["summary"], final_set, month_label=month_label, generated_by=generated_by)
    c1, c2, c3 = st.columns(3)
    c1.download_button("Descargar CSV", data=csv_bytes, file_name=f"red_flags_{month_label}.csv", mime="text/csv")
    c2.download_button("Descargar Excel", data=excel_bytes, file_name=f"red_flags_{month_label}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    c3.download_button("Descargar PDF", data=pdf_bytes, file_name=f"red_flags_{month_label}.pdf", mime="application/pdf")


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
