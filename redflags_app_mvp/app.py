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
from src.normalization import build_agent_key, load_alias_mapping
from src.parsers import (
    SOURCE_MODE_MONTHLY_AUDIT,
    SOURCE_MODE_WEEKLY_DETAIL,
    filter_frames_by_source_mode,
    load_excel_sheets,
    load_selected_frames,
    parse_appointments_frames,
    parse_production_frames,
    preview_columns,
)
from src.monitoring import build_final_monitoring_set
from src.persistence import (
    create_operational_record,
    delete_operational_record,
    load_agent_catalog,
    load_appointment_daily_facts,
    load_audit_log,
    load_manual_appointments_weekly,
    load_monitoring_overrides,
    load_operational_audit_log,
    load_unified_operational_dataset,
    persist_run,
    save_appointment_daily_fact,
    save_monitoring_override,
    update_operational_record,
)
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
        strict_weekly_threshold = st.number_input(
            "Umbral semanal estricto",
            min_value=0.0,
            value=float(DEFAULT_THRESHOLDS.weekly_production_strict),
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
        appointments_merge_rule = st.selectbox(
            "Regla de combinación citas (Excel + carga manual)",
            options=["overwrite", "sum"],
            format_func=lambda value: "Manual sobreescribe Excel" if value == "overwrite" else "Manual + Excel (sumar)",
            index=0,
        )

    st.session_state["generated_by"] = generated_by
    st.session_state["month_label"] = month_label
    st.session_state["alias_file"] = alias_file
    st.session_state["appointments_merge_rule"] = appointments_merge_rule
    return ThresholdConfig(
        monthly_production_suspicious=monthly_threshold,
        weekly_production_suspicious=weekly_threshold,
        weekly_production_strict=strict_weekly_threshold,
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
        selected_source_modes = st.multiselect(
            "source_mode",
            options=[SOURCE_MODE_WEEKLY_DETAIL, SOURCE_MODE_MONTHLY_AUDIT],
            default=[SOURCE_MODE_WEEKLY_DETAIL],
            help="Define qué origen de citas usar para el procesamiento.",
            key="source_mode",
        )
        source_mode = (
            selected_source_modes[0]
            if len(selected_source_modes) == 1
            else SOURCE_MODE_WEEKLY_DETAIL
        )
        if len(selected_source_modes) > 1:
            st.warning(
                "Seleccionaste ambos source_mode. Debes definir una prioridad explícita."
            )
            source_mode = st.radio(
                "Prioridad source_mode",
                options=[SOURCE_MODE_WEEKLY_DETAIL, SOURCE_MODE_MONTHLY_AUDIT],
                key="source_mode_priority",
            )
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
        if not selected_source_modes:
            st.error("Debes seleccionar al menos un source_mode.")
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
        filtered_appointments_frames = filter_frames_by_source_mode(
            appointments_frames, source_mode
        )
        if not filtered_appointments_frames:
            expected_sheet = (
                "reporte de citas abril"
                if source_mode == SOURCE_MODE_WEEKLY_DETAIL
                else "AUDITORIA"
            )
            st.error(
                f"No se encontró la hoja requerida para source_mode={source_mode}: {expected_sheet}."
            )
            return
        raw_appointments = parse_appointments_frames(
            filtered_appointments_frames,
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
        manual_appointments = load_manual_appointments_weekly(st.session_state["month_label"])
        results = run_pipeline(
            raw_production,
            raw_appointments,
            config,
            alias_mapping=alias_mapping,
            manual_appointments=manual_appointments,
            appointments_merge_rule=st.session_state.get("appointments_merge_rule", "overwrite"),
        )
        run_id = persist_run(
            month_label=st.session_state["month_label"],
            generated_by=st.session_state["generated_by"],
            source_mode=source_mode,
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
            conflicts_df=results.get("conflicts"),
        )
        st.session_state["results"] = results
        st.session_state["quality_summary"] = build_quality_summary(
            raw_production, raw_appointments
        )
        st.success(f"Procesamiento completado. Run ID: {run_id}.")


def render_operational_registry() -> None:
    st.subheader("Registro operativo")
    generated_by = st.session_state.get("generated_by", "operador")
    with st.expander("A) Citas diarias", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        agent_name = c1.text_input("Agente", key="op_appt_agent")
        record_date = c2.date_input("Fecha", key="op_appt_date")
        amount = c3.number_input("Cantidad", min_value=0.0, step=1.0, key="op_appt_qty")
        notes = c4.text_input("Notas", key="op_appt_notes")
        if st.button("Guardar cita diaria"):
            create_operational_record(
                record_type="appointments",
                agent_name=agent_name,
                record_date=str(record_date),
                amount=amount,
                load_type="diaria",
                notes=notes,
                source_origin="manual",
                source_detail="form_citas_diarias",
                created_by=generated_by,
            )
            st.success("Cita diaria registrada")

    with st.expander("B) Producción", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        prod_agent = c1.text_input("Agente", key="op_prod_agent")
        prod_date = c2.date_input("Fecha", key="op_prod_date")
        load_type = c3.selectbox("Tipo de carga", ["diaria", "semanal", "mensual"], key="op_prod_load")
        prod_amount = c4.number_input("Monto", min_value=0.0, step=100.0, key="op_prod_amount")
        if st.button("Guardar producción"):
            create_operational_record(
                record_type="production",
                agent_name=prod_agent,
                record_date=str(prod_date),
                amount=prod_amount,
                load_type=load_type,
                notes="",
                source_origin="manual",
                source_detail="form_produccion",
                created_by=generated_by,
            )
            st.success("Producción registrada")

    with st.expander("Importación masiva CSV (opcional)", expanded=False):
        st.caption("Columnas requeridas: record_type, agent_name, record_date, amount. Opcionales: load_type, notes")
        csv_file = st.file_uploader("Subir CSV", type=["csv"], key="operational_csv")
        if csv_file is not None and st.button("Importar CSV"):
            csv_df = pd.read_csv(csv_file)
            required = {"record_type", "agent_name", "record_date", "amount"}
            missing = required.difference(csv_df.columns)
            if missing:
                st.error(f"Faltan columnas en CSV: {', '.join(sorted(missing))}")
            else:
                for _, row in csv_df.iterrows():
                    create_operational_record(
                        record_type=str(row.get("record_type", "")).strip(),
                        agent_name=str(row.get("agent_name", "")).strip(),
                        record_date=str(row.get("record_date", "")).strip(),
                        amount=float(row.get("amount", 0)),
                        load_type=str(row.get("load_type", "diaria")).strip() or "diaria",
                        notes=str(row.get("notes", "")).strip(),
                        source_origin="csv",
                        source_detail=getattr(csv_file, "name", "operational.csv"),
                        created_by=generated_by,
                    )
                st.success(f"Importación completada: {len(csv_df)} registros")

    month_default = st.session_state.get("month_label", DEFAULT_MONTH)
    f1, f2 = st.columns(2)
    agent_filter = f1.text_input("Filtro por agente", key="op_filter_agent")
    date_filter = f2.text_input("Filtro por fecha (YYYY-MM o YYYY-MM-DD)", value=month_default, key="op_filter_date")
    records = load_unified_operational_dataset()
    if not records.empty:
        if agent_filter:
            records = records[records["agent_name"].astype(str).str.contains(agent_filter, case=False, na=False)]
        if date_filter:
            records = records[records["record_date"].astype(str).str.startswith(date_filter)]

    st.markdown("#### Tabla editable del día")
    if records.empty:
        st.info("No hay registros operativos para el filtro seleccionado")
        return

    st.dataframe(
        records[["id", "record_type", "agent_name", "record_date", "amount", "load_type", "notes", "source_origin", "origin_trace"]],
        use_container_width=True,
    )

    st.markdown("#### Corrección de registros (con auditoría)")
    rec_ids = records["id"].astype(int).tolist()
    selected_id = st.selectbox("Registro", options=rec_ids)
    selected_row = records[records["id"] == selected_id].iloc[0]
    e1, e2, e3 = st.columns(3)
    edit_amount = e1.number_input("Monto corregido", min_value=0.0, value=float(selected_row["amount"]))
    edit_load = e2.selectbox("Tipo de carga", ["diaria", "semanal", "mensual"], index=["diaria", "semanal", "mensual"].index(str(selected_row["load_type"]) if str(selected_row["load_type"]) in ["diaria", "semanal", "mensual"] else "diaria"))
    edit_notes = e3.text_input("Notas corregidas", value=str(selected_row.get("notes", "")))
    c_upd, c_del = st.columns(2)
    if c_upd.button("Guardar corrección", use_container_width=True):
        update_operational_record(
            record_id=int(selected_id),
            amount=float(edit_amount),
            notes=edit_notes,
            load_type=edit_load,
            performed_by=generated_by,
        )
        st.success("Registro actualizado")
    if c_del.button("Eliminar registro", use_container_width=True):
        delete_operational_record(record_id=int(selected_id), performed_by=generated_by)
        st.success("Registro eliminado")


def render_manual_load() -> None:
    st.subheader("Carga manual")
    generated_by = st.session_state.get("generated_by", "operador")
    default_month_date = datetime.now().replace(day=1).date()
    month_date = st.date_input("Mes de trabajo", value=default_month_date, format="YYYY/MM/DD", key="manual_month_date")
    selected_month = pd.Timestamp(month_date).strftime("%Y-%m")

    catalog = load_agent_catalog()
    labels = [row.agent_name for _, row in catalog.iterrows()] if not catalog.empty else []
    selected_label = st.selectbox("Agente (catálogo)", options=[""] + labels)

    selected_row = None
    if selected_label and not catalog.empty:
        selected_row = catalog.iloc[labels.index(selected_label)]

    agent_name = st.text_input("Nombre de agente", value=selected_row["agent_name"] if selected_row is not None else "", key="manual_agent_name")
    agent_code = st.text_input("Código de agente (opcional)", value=selected_row["agent_code"] if selected_row is not None and "agent_code" in selected_row else "", key="manual_agent_code")
    appointment_date = st.date_input("Fecha del día", value=datetime.now().date(), key="manual_appointment_date")
    appointment_count = st.number_input("Cantidad de citas", min_value=0.0, step=1.0, key="manual_appointment_count")

    if st.button("Guardar", type="primary", key="manual_save"):
        if not agent_name.strip():
            st.error("Debes indicar un agente.")
            return
        agent_key = build_agent_key(agent_name=agent_name, hierarchy="", agent_code=agent_code)
        save_appointment_daily_fact(
            agent_key=agent_key,
            agent_code=agent_code,
            agent_name=agent_name,
            appointment_date=pd.Timestamp(appointment_date).strftime("%Y-%m-%d"),
            appointment_count=float(appointment_count),
            source="manual",
            created_by=generated_by,
        )
        st.success("Carga manual guardada correctamente.")

    st.markdown("#### Detalle diario para auditoría")
    daily = load_appointment_daily_facts(month_label=selected_month)
    if daily.empty:
        st.info("No hay registros manuales en el mes seleccionado.")
    else:
        st.dataframe(daily, use_container_width=True)


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
    if "production_monthly_total" in flagged.columns:
        flagged = flagged[flagged["production_monthly_total"] > 0].copy()

    overrides = load_monitoring_overrides(month_filter if month_filter != "Todos" else None)
    selected_month = month_filter if month_filter != "Todos" else (summary["month"].iloc[0] if not summary.empty else "")
    final_set = build_final_monitoring_set(summary, results["flags"], overrides, selected_month) if selected_month else pd.DataFrame()

    st.markdown("#### Controles de monitoreo manual")
    if not summary.empty:
        agent_options = {f"{r.agent_name} ({r.agent_key})": r.agent_key for _, r in summary.drop_duplicates(subset=["agent_key"]).iterrows()}
        selected_label = st.selectbox("Agente para incluir/excluir", options=list(agent_options.keys()) if agent_options else [])
        reason = st.text_input("Razón manual")
        c_inc, c_exc = st.columns(2)
        if c_inc.button("Incluir en reporte", use_container_width=True) and selected_label and reason:
            save_monitoring_override(agent_key=agent_options[selected_label], report_month=selected_month, action_type="include", reason=reason, created_by=st.session_state.get("generated_by", "operador"))
            st.success("Inclusión manual guardada")
        if c_exc.button("Excluir del reporte", use_container_width=True) and selected_label and reason:
            save_monitoring_override(agent_key=agent_options[selected_label], report_month=selected_month, action_type="exclude", reason=reason, created_by=st.session_state.get("generated_by", "operador"))
            st.success("Exclusión manual guardada")

    st.markdown("#### Lista final previa a PDF")
    if final_set.empty:
        st.info("No hay agentes en el conjunto final para el mes seleccionado.")
    else:
        final_set["selected_for_pdf"] = True
        edited = st.data_editor(final_set[["selected_for_pdf", "agent_name", "hierarchy", "appointments_month_total", "production_monthly_total", "active_flags", "inclusion_reason"]], use_container_width=True, key="final_pdf_editor")
        st.session_state["final_pdf_set"] = final_set.loc[edited["selected_for_pdf"]].copy()
        st.session_state["final_pdf_month"] = selected_month

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

    unified = load_unified_operational_dataset(selected_month if selected_month else None)
    st.markdown("#### Dataset operativo unificado (manual + excel + csv)")
    if unified.empty:
        st.info("Sin registros operativos para el período seleccionado")
    else:
        u1, u2 = st.columns(2)
        u1.metric("Registros operativos", f"{len(unified)}")
        u2.metric("Agentes en operativo", f"{unified['agent_name'].nunique()}")
        ops_summary = (
            unified.groupby(["record_type", "source_origin"], as_index=False)
            .agg(registros=("id", "count"), monto_total=("amount", "sum"))
            .sort_values(["record_type", "source_origin"])
        )
        st.dataframe(ops_summary, use_container_width=True)

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
    conflicts = results.get("conflicts")
    if quality is not None:
        st.markdown("#### Resumen de calidad de datos")
        st.dataframe(quality, use_container_width=True)
    if conflicts is not None and not conflicts.empty:
        st.markdown("#### Conflictos de importación detectados")
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
    overrides = load_monitoring_overrides(month_label)
    default_final = build_final_monitoring_set(results["summary"], results["flags"], overrides, month_label)
    final_for_pdf = st.session_state.get("final_pdf_set")
    final_pdf_month = st.session_state.get("final_pdf_month")
    if final_for_pdf is None or final_pdf_month != month_label or final_for_pdf.empty:
        final_for_pdf = default_final
    if "production_monthly_total" in final_for_pdf.columns:
        final_for_pdf = final_for_pdf[final_for_pdf["production_monthly_total"] > 0].copy()
    unified = load_unified_operational_dataset(month_label)
    pdf_bytes = build_pdf_report(
        final_for_pdf,
        results["flags"],
        month_label=month_label,
        generated_by=generated_by,
        unified_operational=unified,
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
    st.markdown("#### Corridas")
    st.dataframe(load_audit_log(), use_container_width=True)
    st.markdown("#### Auditoría de correcciones operativas")
    st.dataframe(load_operational_audit_log(), use_container_width=True)


def main() -> None:
    st.title(APP_TITLE)
    config = build_threshold_config()
    tabs = st.tabs(["Carga", "Carga manual", "Registro operativo", "Dashboard", "Detalle", "Reportes", "Histórico"])
    with tabs[0]:
        render_upload_and_process(config)
    with tabs[1]:
        render_manual_load()
    with tabs[2]:
        render_operational_registry()
    with tabs[3]:
        render_dashboard()
    with tabs[4]:
        render_agent_detail()
    with tabs[5]:
        render_reports()
    with tabs[6]:
        render_history()


if __name__ == "__main__":
    main()
