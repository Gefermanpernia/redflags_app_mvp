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
                CREATE TABLE IF NOT EXISTS facts_daily (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fact_date TEXT NOT NULL,
                    month TEXT NOT NULL,
                    week INTEGER NOT NULL,
                    agent_name TEXT NOT NULL,
                    hierarchy TEXT,
                    agent_code TEXT,
                    appointments REAL NOT NULL DEFAULT 0,
                    production REAL NOT NULL DEFAULT 0,
                    source_type TEXT NOT NULL DEFAULT 'manual',
                    source_ref TEXT,
                    notes TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
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
    return run_id


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


def save_daily_fact(
    *,
    fact_date: str,
    agent_name: str,
    hierarchy: str,
    agent_code: str,
    appointments: float,
    production: float,
    notes: str,
    created_by: str,
) -> None:
    _init_db()
    parsed_date = datetime.fromisoformat(fact_date)
    month = parsed_date.strftime("%Y-%m")
    week = int((parsed_date.day - 1) // 7 + 1)
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO facts_daily
                (fact_date, month, week, agent_name, hierarchy, agent_code, appointments, production,
                 source_type, source_ref, notes, created_by, created_at)
                VALUES (:fact_date, :month, :week, :agent_name, :hierarchy, :agent_code, :appointments, :production,
                        'manual', 'streamlit_manual', :notes, :created_by, :created_at)
                """
            ),
            {
                "fact_date": parsed_date.date().isoformat(),
                "month": month,
                "week": week,
                "agent_name": agent_name,
                "hierarchy": hierarchy,
                "agent_code": agent_code,
                "appointments": float(appointments),
                "production": float(production),
                "notes": notes,
                "created_by": created_by,
                "created_at": datetime.utcnow().isoformat(),
            },
        )


def load_daily_facts(month_label: str | None = None) -> pd.DataFrame:
    _init_db()
    if month_label:
        return pd.read_sql(
            text("SELECT * FROM facts_daily WHERE month = :month ORDER BY fact_date DESC, id DESC"),
            ENGINE,
            params={"month": month_label},
        )
    return pd.read_sql("SELECT * FROM facts_daily ORDER BY fact_date DESC, id DESC", ENGINE)
