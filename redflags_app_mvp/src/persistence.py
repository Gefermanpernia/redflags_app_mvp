from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORY_DIR = BASE_DIR / "history"
AUDIT_FILE = BASE_DIR / "audit_log.csv"



def persist_run(
    *,
    month_label: str,
    generated_by: str,
    raw_production: pd.DataFrame,
    raw_appointments: pd.DataFrame,
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    flags_df: pd.DataFrame,
    summary_df: pd.DataFrame,
) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    month_slug = month_label.replace("/", "-").replace(" ", "_")
    target_dir = HISTORY_DIR / month_slug / timestamp
    target_dir.mkdir(parents=True, exist_ok=True)

    raw_production.to_csv(target_dir / "raw_production.csv", index=False)
    raw_appointments.to_csv(target_dir / "raw_appointments.csv", index=False)
    weekly_df.to_csv(target_dir / "weekly.csv", index=False)
    monthly_df.to_csv(target_dir / "monthly.csv", index=False)
    flags_df.to_csv(target_dir / "flags.csv", index=False)
    summary_df.to_csv(target_dir / "summary.csv", index=False)

    audit_row = pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "month_label": month_label,
                "generated_by": generated_by,
                "raw_production_rows": len(raw_production),
                "raw_appointments_rows": len(raw_appointments),
                "weekly_rows": len(weekly_df),
                "flags_rows": len(flags_df),
                "path": str(target_dir),
            }
        ]
    )

    if AUDIT_FILE.exists():
        history = pd.read_csv(AUDIT_FILE)
        history = pd.concat([history, audit_row], ignore_index=True)
    else:
        history = audit_row
    history.to_csv(AUDIT_FILE, index=False)
    return target_dir



def load_audit_log() -> pd.DataFrame:
    if AUDIT_FILE.exists():
        return pd.read_csv(AUDIT_FILE)
    return pd.DataFrame(
        columns=[
            "timestamp",
            "month_label",
            "generated_by",
            "raw_production_rows",
            "raw_appointments_rows",
            "weekly_rows",
            "flags_rows",
            "path",
        ]
    )
