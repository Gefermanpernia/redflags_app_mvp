from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parent.parent / "data"
BASE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = BASE_DIR / "redflags.sqlite"
ENGINE = create_engine(f"sqlite:///{DB_PATH}")


def _init_db() -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS run_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    month_label TEXT NOT NULL,
                    generated_by TEXT NOT NULL,
                    source_mode TEXT NOT NULL DEFAULT 'weekly_detail',
                    production_file_name TEXT,
                    appointments_file_name TEXT,
                    raw_production_rows INTEGER,
                    raw_appointments_rows INTEGER,
                    weekly_rows INTEGER,
                    flags_rows INTEGER
                )
                """
            )
        )
        columns = conn.execute(text("PRAGMA table_info(run_audit)")).fetchall()
        column_names = {row[1] for row in columns}
        if "source_mode" not in column_names:
            conn.execute(
                text(
                    "ALTER TABLE run_audit ADD COLUMN source_mode TEXT NOT NULL DEFAULT 'weekly_detail'"
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS file_trace (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    dataset_type TEXT NOT NULL,
                    file_name TEXT,
                    source_sheet TEXT,
                    row_count INTEGER,
                    generated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES run_audit(id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS monitoring_overrides (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_key TEXT NOT NULL,
                    report_month TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operational_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    record_type TEXT NOT NULL,
                    agent_name TEXT NOT NULL,
                    record_date TEXT NOT NULL,
                    month_label TEXT NOT NULL,
                    amount REAL NOT NULL,
                    load_type TEXT,
                    notes TEXT,
                    source_origin TEXT NOT NULL,
                    source_detail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    deleted_at TEXT,
                    created_by TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES run_audit(id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operational_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    action_type TEXT NOT NULL,
                    payload_json TEXT,
                    performed_by TEXT NOT NULL,
                    performed_at TEXT NOT NULL
                )
                """
            )
        )


def _soft_delete_previous_excel_records(month_label: str, source_detail: str) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE operational_records
                SET deleted_at = :deleted_at
                WHERE source_origin = 'excel'
                  AND month_label = :month_label
                  AND source_detail = :source_detail
                  AND deleted_at IS NULL
                """
            ),
            {
                "deleted_at": datetime.utcnow().isoformat(),
                "month_label": month_label,
                "source_detail": source_detail,
            },
        )


def _append_operational_records(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    frame = frame.copy()
    frame.to_sql("operational_records", ENGINE, if_exists="append", index=False)


def _log_operational_action(record_id: int | None, action_type: str, payload: dict, performed_by: str) -> None:
    payload_json = pd.Series([payload]).to_json(orient="records")
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operational_audit_log
                (record_id, action_type, payload_json, performed_by, performed_at)
                VALUES (:record_id, :action_type, :payload_json, :performed_by, :performed_at)
                """
            ),
            {
                "record_id": record_id,
                "action_type": action_type,
                "payload_json": payload_json,
                "performed_by": performed_by,
                "performed_at": datetime.utcnow().isoformat(),
            },
        )


def persist_run(
    *,
    month_label: str,
    generated_by: str,
    source_mode: str,
    production_file_name: str,
    appointments_file_name: str,
    raw_production: pd.DataFrame,
    raw_appointments: pd.DataFrame,
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    flags_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    conflicts_df: pd.DataFrame | None = None,
) -> int:
    _init_db()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with ENGINE.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO run_audit
                (timestamp, month_label, generated_by, source_mode, production_file_name, appointments_file_name,
                 raw_production_rows, raw_appointments_rows, weekly_rows, flags_rows)
                VALUES (:timestamp, :month_label, :generated_by, :source_mode, :production_file_name, :appointments_file_name,
                        :raw_production_rows, :raw_appointments_rows, :weekly_rows, :flags_rows)
                """
            ),
            {
                "timestamp": timestamp,
                "month_label": month_label,
                "generated_by": generated_by,
                "source_mode": source_mode,
                "production_file_name": production_file_name,
                "appointments_file_name": appointments_file_name,
                "raw_production_rows": len(raw_production),
                "raw_appointments_rows": len(raw_appointments),
                "weekly_rows": len(weekly_df),
                "flags_rows": len(flags_df),
            },
        )
        run_id = int(result.lastrowid)

    for dataset_type, file_name, frame in (
        ("production", production_file_name, raw_production),
        ("appointments", appointments_file_name, raw_appointments),
    ):
        if frame.empty:
            continue
        traces = frame.groupby("source_sheet", as_index=False).size().rename(columns={"size": "row_count", "source_sheet": "source_sheet"})
        traces["run_id"] = run_id
        traces["dataset_type"] = dataset_type
        traces["file_name"] = file_name
        traces["generated_by"] = generated_by
        traces["created_at"] = timestamp
        traces.to_sql("file_trace", ENGINE, if_exists="append", index=False)

    weekly_df.assign(run_id=run_id).to_sql("weekly_results", ENGINE, if_exists="append", index=False)
    monthly_df.assign(run_id=run_id).to_sql("monthly_results", ENGINE, if_exists="append", index=False)
    flags_df.assign(run_id=run_id).to_sql("flags_results", ENGINE, if_exists="append", index=False)
    summary_df.assign(run_id=run_id).to_sql("summary_results", ENGINE, if_exists="append", index=False)
    if conflicts_df is not None and not conflicts_df.empty:
        conflicts_df.assign(run_id=run_id).to_sql("import_conflicts", ENGINE, if_exists="append", index=False)

    created_at = datetime.utcnow().isoformat()
    _soft_delete_previous_excel_records(month_label, appointments_file_name)
    _soft_delete_previous_excel_records(month_label, production_file_name)

    if not raw_appointments.empty:
        appointments_records = raw_appointments[["agent_name", "month", "appointments"]].copy()
        appointments_records["record_date"] = pd.to_datetime(appointments_records["month"].astype(str) + "-01", errors="coerce").dt.strftime("%Y-%m-%d")
        appointments_records["month_label"] = appointments_records["month"].astype(str)
        appointments_records["record_type"] = "appointments"
        appointments_records["amount"] = pd.to_numeric(appointments_records["appointments"], errors="coerce").fillna(0)
        appointments_records["load_type"] = "diaria"
        appointments_records["notes"] = "Importado desde Excel de citas"
        appointments_records["source_origin"] = "excel"
        appointments_records["source_detail"] = appointments_file_name
        appointments_records["created_by"] = generated_by
        appointments_records["created_at"] = created_at
        appointments_records["updated_at"] = None
        appointments_records["deleted_at"] = None
        appointments_records["run_id"] = run_id
        _append_operational_records(
            appointments_records[
                [
                    "run_id", "record_type", "agent_name", "record_date", "month_label", "amount", "load_type", "notes",
                    "source_origin", "source_detail", "created_at", "updated_at", "deleted_at", "created_by",
                ]
            ]
        )

    if not raw_production.empty:
        production_records = raw_production[["agent_name", "month", "production_mtd"]].copy()
        production_records["record_date"] = pd.to_datetime(production_records["month"].astype(str) + "-01", errors="coerce").dt.strftime("%Y-%m-%d")
        production_records["month_label"] = production_records["month"].astype(str)
        production_records["record_type"] = "production"
        production_records["amount"] = pd.to_numeric(production_records["production_mtd"], errors="coerce").fillna(0)
        production_records["load_type"] = "mensual"
        production_records["notes"] = "Importado desde Excel de producción"
        production_records["source_origin"] = "excel"
        production_records["source_detail"] = production_file_name
        production_records["created_by"] = generated_by
        production_records["created_at"] = created_at
        production_records["updated_at"] = None
        production_records["deleted_at"] = None
        production_records["run_id"] = run_id
        _append_operational_records(
            production_records[
                [
                    "run_id", "record_type", "agent_name", "record_date", "month_label", "amount", "load_type", "notes",
                    "source_origin", "source_detail", "created_at", "updated_at", "deleted_at", "created_by",
                ]
            ]
        )
    return run_id


def create_operational_record(
    *,
    record_type: str,
    agent_name: str,
    record_date: str,
    amount: float,
    load_type: str,
    notes: str,
    source_origin: str,
    source_detail: str,
    created_by: str,
) -> None:
    _init_db()
    record_date_dt = pd.to_datetime(record_date, errors="coerce")
    if pd.isna(record_date_dt):
        raise ValueError("Fecha inválida para registro operativo")
    payload = {
        "run_id": None,
        "record_type": record_type,
        "agent_name": str(agent_name).strip(),
        "record_date": record_date_dt.strftime("%Y-%m-%d"),
        "month_label": record_date_dt.strftime("%Y-%m"),
        "amount": float(amount),
        "load_type": load_type,
        "notes": notes,
        "source_origin": source_origin,
        "source_detail": source_detail,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": None,
        "deleted_at": None,
        "created_by": created_by,
    }
    with ENGINE.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO operational_records
                (run_id, record_type, agent_name, record_date, month_label, amount, load_type, notes, source_origin,
                 source_detail, created_at, updated_at, deleted_at, created_by)
                VALUES (:run_id, :record_type, :agent_name, :record_date, :month_label, :amount, :load_type, :notes, :source_origin,
                        :source_detail, :created_at, :updated_at, :deleted_at, :created_by)
                """
            ),
            payload,
        )
        record_id = int(result.lastrowid)
    _log_operational_action(record_id, "create", payload, created_by)


def update_operational_record(*, record_id: int, amount: float, notes: str, load_type: str, performed_by: str) -> None:
    _init_db()
    payload = {
        "record_id": int(record_id),
        "amount": float(amount),
        "notes": notes,
        "load_type": load_type,
        "updated_at": datetime.utcnow().isoformat(),
    }
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE operational_records
                SET amount = :amount,
                    notes = :notes,
                    load_type = :load_type,
                    updated_at = :updated_at
                WHERE id = :record_id AND deleted_at IS NULL
                """
            ),
            payload,
        )
    _log_operational_action(record_id, "update", payload, performed_by)


def delete_operational_record(*, record_id: int, performed_by: str) -> None:
    _init_db()
    payload = {"record_id": int(record_id), "deleted_at": datetime.utcnow().isoformat()}
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE operational_records
                SET deleted_at = :deleted_at
                WHERE id = :record_id AND deleted_at IS NULL
                """
            ),
            payload,
        )
    _log_operational_action(record_id, "delete", payload, performed_by)


def load_operational_records(month_label: str | None = None) -> pd.DataFrame:
    _init_db()
    query = "SELECT * FROM operational_records WHERE deleted_at IS NULL"
    params: dict[str, str] = {}
    if month_label:
        query += " AND month_label = :month_label"
        params["month_label"] = month_label
    query += " ORDER BY record_date DESC, id DESC"
    return pd.read_sql(text(query), ENGINE, params=params)


def load_unified_operational_dataset(month_label: str | None = None) -> pd.DataFrame:
    records = load_operational_records(month_label)
    if records.empty:
        return records
    dedupe_keys = ["record_type", "agent_name", "record_date", "amount", "source_origin", "source_detail"]
    records = records.sort_values(["id"], ascending=False).drop_duplicates(subset=dedupe_keys, keep="first")
    records["origin_trace"] = records["source_origin"].astype(str) + " | " + records["source_detail"].fillna("")
    return records.sort_values(["record_date", "id"], ascending=[False, False]).reset_index(drop=True)


def load_operational_audit_log() -> pd.DataFrame:
    _init_db()
    return pd.read_sql("SELECT * FROM operational_audit_log ORDER BY id DESC", ENGINE)


def _week_of_month(date_value: str) -> int:
    dt = pd.to_datetime(date_value, errors="coerce")
    if pd.isna(dt):
        return 1
    return max(1, min(5, int(((dt.day - 1) // 7) + 1)))


def load_agent_catalog() -> pd.DataFrame:
    _init_db()
    records = load_operational_records()
    if records.empty:
        return pd.DataFrame(columns=["agent_name", "agent_code"])
    out = records[["agent_name"]].dropna().drop_duplicates().copy()
    out["agent_code"] = ""
    return out.sort_values(["agent_name"]).reset_index(drop=True)


def save_appointment_daily_fact(
    *,
    agent_key: str,
    agent_code: str,
    agent_name: str,
    appointment_date: str,
    appointment_count: float,
    source: str,
    created_by: str,
) -> None:
    create_operational_record(
        record_type="appointments",
        agent_name=agent_name,
        record_date=appointment_date,
        amount=appointment_count,
        load_type="diaria",
        notes=f"agent_key={agent_key};agent_code={agent_code}",
        source_origin=source,
        source_detail="manual_daily_fact",
        created_by=created_by,
    )


def load_appointment_daily_facts(month_label: str | None = None) -> pd.DataFrame:
    records = load_operational_records(month_label)
    if records.empty:
        return pd.DataFrame()
    records = records[(records["record_type"] == "appointments") & (records["source_origin"].isin(["manual", "csv"]))].copy()
    return records.sort_values(["record_date", "id"], ascending=[False, False]).reset_index(drop=True)


def load_manual_appointments_weekly(month_label: str) -> pd.DataFrame:
    daily = load_appointment_daily_facts(month_label)
    if daily.empty:
        return pd.DataFrame(columns=["month", "week", "agent_name", "agent_code", "hierarchy", "appointments", "source_sheet"])
    daily["week"] = daily["record_date"].apply(_week_of_month)
    weekly = (
        daily.groupby(["month_label", "week", "agent_name"], as_index=False)
        .agg(appointments=("amount", "sum"))
        .rename(columns={"month_label": "month"})
    )
    weekly["agent_code"] = ""
    weekly["hierarchy"] = ""
    weekly["source_sheet"] = "manual_daily_fact"
    return weekly[["month", "week", "agent_name", "agent_code", "hierarchy", "appointments", "source_sheet"]]


def save_monitoring_override(
    *, agent_key: str, report_month: str, action_type: str, reason: str, created_by: str
) -> None:
    _init_db()
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO monitoring_overrides
                (agent_key, report_month, action_type, reason, created_at, created_by)
                VALUES (:agent_key, :report_month, :action_type, :reason, :created_at, :created_by)
                """
            ),
            {
                "agent_key": agent_key,
                "report_month": report_month,
                "action_type": action_type,
                "reason": reason,
                "created_at": datetime.utcnow().isoformat(),
                "created_by": created_by,
            },
        )


def load_monitoring_overrides(report_month: str | None = None) -> pd.DataFrame:
    _init_db()
    if report_month:
        return pd.read_sql(
            text("SELECT * FROM monitoring_overrides WHERE report_month = :report_month ORDER BY id DESC"),
            ENGINE,
            params={"report_month": report_month},
        )
    return pd.read_sql("SELECT * FROM monitoring_overrides ORDER BY id DESC", ENGINE)


def load_audit_log() -> pd.DataFrame:
    _init_db()
    return pd.read_sql("SELECT * FROM run_audit ORDER BY id DESC", ENGINE)
