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
        conn.execute(text("""
                CREATE TABLE IF NOT EXISTS run_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    month_label TEXT NOT NULL,
                    generated_by TEXT NOT NULL,
                    production_file_name TEXT,
                    appointments_file_name TEXT,
                    raw_production_rows INTEGER,
                    raw_appointments_rows INTEGER,
                    weekly_rows INTEGER,
                    flags_rows INTEGER,
                    conflict_rows INTEGER DEFAULT 0
                )
                """))
        conn.execute(text("""
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
                """))
        conn.execute(text("""
                CREATE TABLE IF NOT EXISTS manual_monitoring_override (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_key TEXT NOT NULL,
                    report_month TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL
                )
                """))


def persist_override(agent_key: str, report_month: str, action_type: str, reason: str, created_by: str) -> None:
    _init_db()
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO manual_monitoring_override
                (agent_key, report_month, action_type, reason, created_at, created_by)
                VALUES (:agent_key, :report_month, :action_type, :reason, :created_at, :created_by)
            """),
            {
                "agent_key": agent_key,
                "report_month": report_month,
                "action_type": action_type,
                "reason": reason,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "created_by": created_by,
            },
        )


def load_overrides(report_month: str | None = None) -> pd.DataFrame:
    _init_db()
    query = "SELECT * FROM manual_monitoring_override"
    if report_month:
        query += " WHERE report_month = :report_month"
        return pd.read_sql(text(query), ENGINE, params={"report_month": report_month})
    return pd.read_sql(query, ENGINE)


def persist_run(
    *,
    month_label: str,
    generated_by: str,
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
            text("""
                INSERT INTO run_audit
                (timestamp, month_label, generated_by, production_file_name, appointments_file_name,
                 raw_production_rows, raw_appointments_rows, weekly_rows, flags_rows, conflict_rows)
                VALUES (:timestamp, :month_label, :generated_by, :production_file_name, :appointments_file_name,
                        :raw_production_rows, :raw_appointments_rows, :weekly_rows, :flags_rows, :conflict_rows)
                """),
            {
                "timestamp": timestamp,
                "month_label": month_label,
                "generated_by": generated_by,
                "production_file_name": production_file_name,
                "appointments_file_name": appointments_file_name,
                "raw_production_rows": len(raw_production),
                "raw_appointments_rows": len(raw_appointments),
                "weekly_rows": len(weekly_df),
                "flags_rows": len(flags_df),
                "conflict_rows": len(conflicts_df) if conflicts_df is not None else 0,
            },
        )
        run_id = int(result.lastrowid)

    for dataset_type, file_name, frame in (("production", production_file_name, raw_production), ("appointments", appointments_file_name, raw_appointments)):
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
    return run_id


def load_audit_log() -> pd.DataFrame:
    _init_db()
    return pd.read_sql("SELECT * FROM run_audit ORDER BY id DESC", ENGINE)
