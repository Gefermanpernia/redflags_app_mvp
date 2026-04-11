from __future__ import annotations

from typing import Dict

import pandas as pd

from .config import ThresholdConfig
from .metrics import build_monthly_dataset, build_summary_table, build_weekly_dataset
from .red_flags import evaluate_red_flags


def apply_monitoring_overrides(
    summary: pd.DataFrame,
    flags: pd.DataFrame,
    month_label: str,
    overrides: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if summary.empty:
        return summary
    tracked = summary.copy()
    auto_set = set(flags[flags["month"] == month_label]["agent_key"].unique().tolist()) if not flags.empty else set()
    tracked["auto_red_flag"] = tracked["agent_key"].isin(auto_set)
    tracked["manual_include"] = False
    tracked["manual_exclude"] = False
    tracked["include_reason"] = ""
    tracked["exclude_reason"] = ""

    if overrides is not None and not overrides.empty:
        scoped = overrides[overrides["report_month"] == month_label]
        include_map = (
            scoped[scoped["action_type"] == "include"].sort_values("created_at").groupby("agent_key").tail(1)
        )
        exclude_map = (
            scoped[scoped["action_type"] == "exclude"].sort_values("created_at").groupby("agent_key").tail(1)
        )
        if not include_map.empty:
            includes = include_map.set_index("agent_key")["reason"].to_dict()
            tracked.loc[tracked["agent_key"].isin(includes.keys()), "manual_include"] = True
            tracked.loc[tracked["agent_key"].isin(includes.keys()), "include_reason"] = tracked["agent_key"].map(includes)
        if not exclude_map.empty:
            excludes = exclude_map.set_index("agent_key")["reason"].to_dict()
            tracked.loc[tracked["agent_key"].isin(excludes.keys()), "manual_exclude"] = True
            tracked.loc[tracked["agent_key"].isin(excludes.keys()), "exclude_reason"] = tracked["agent_key"].map(excludes)

    tracked["monitoring_source"] = tracked.apply(
        lambda row: "both" if row["auto_red_flag"] and row["manual_include"] else (
            "manual" if row["manual_include"] else ("auto" if row["auto_red_flag"] else "none")
        ),
        axis=1,
    )
    tracked["in_final_monitoring_set"] = (tracked["auto_red_flag"] | tracked["manual_include"]) & ~tracked["manual_exclude"]
    return tracked


def run_pipeline(
    raw_production: pd.DataFrame,
    raw_appointments: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
    monitoring_overrides: pd.DataFrame | None = None,
    month_label: str | None = None,
) -> Dict[str, pd.DataFrame]:
    weekly = build_weekly_dataset(raw_production, raw_appointments, config, alias_mapping=alias_mapping)
    monthly = build_monthly_dataset(weekly)
    flags = evaluate_red_flags(weekly, monthly, config)
    summary = build_summary_table(weekly, flags)

    selected_month = month_label or (str(summary["month"].iloc[0]) if not summary.empty else "")
    summary = apply_monitoring_overrides(summary, flags, selected_month, monitoring_overrides)

    flagged_agents = summary[summary["in_final_monitoring_set"]].copy() if not summary.empty else summary
    if not flagged_agents.empty and "risk_score" in flagged_agents.columns:
        flagged_agents = flagged_agents.sort_values(["risk_score", "production_monthly_total"], ascending=[False, False])
    return {"weekly": weekly, "monthly": monthly, "flags": flags, "summary": summary, "flagged_agents": flagged_agents}
