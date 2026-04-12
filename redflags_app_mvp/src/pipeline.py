from __future__ import annotations

from typing import Dict

import pandas as pd

from .config import ThresholdConfig
from .metrics import build_monthly_dataset, build_summary_table, build_weekly_dataset
from .red_flags import evaluate_red_flags


def run_pipeline(
    raw_production: pd.DataFrame,
    raw_appointments: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
    manual_appointments: pd.DataFrame | None = None,
    appointments_merge_rule: str = "overwrite",
) -> Dict[str, pd.DataFrame]:
    merged_appointments = raw_appointments.copy()
    if manual_appointments is not None and not manual_appointments.empty:
        if appointments_merge_rule == "sum":
            merged_appointments = pd.concat([merged_appointments, manual_appointments], ignore_index=True)
        else:
            manual_keys = manual_appointments[["month", "week", "agent_name"]].copy()
            manual_keys["_manual_key"] = manual_keys["month"].astype(str) + "|" + manual_keys["week"].astype(str) + "|" + manual_keys["agent_name"].astype(str).str.strip().str.lower()
            merged_appointments["_manual_key"] = merged_appointments["month"].astype(str) + "|" + merged_appointments["week"].astype(str) + "|" + merged_appointments["agent_name"].astype(str).str.strip().str.lower()
            merged_appointments = merged_appointments[~merged_appointments["_manual_key"].isin(set(manual_keys["_manual_key"]))].drop(columns=["_manual_key"])
            merged_appointments = pd.concat([merged_appointments, manual_appointments], ignore_index=True)

    weekly, conflicts = build_weekly_dataset(
        raw_production, merged_appointments, config, alias_mapping=alias_mapping
    )
    monthly = build_monthly_dataset(weekly)
    flags = evaluate_red_flags(weekly, monthly, config)
    summary = build_summary_table(weekly, flags)

    flagged_agents = (
        summary[summary["active_flags"].astype(str).str.strip() != ""].copy()
        if not summary.empty
        else summary
    )
    if not flagged_agents.empty and "risk_score" in flagged_agents.columns:
        flagged_agents = flagged_agents.sort_values(
            ["risk_score", "production_monthly_total"], ascending=[False, False]
        )
    return {
        "weekly": weekly,
        "monthly": monthly,
        "flags": flags,
        "summary": summary,
        "flagged_agents": flagged_agents,
        "conflicts": conflicts,
    }
