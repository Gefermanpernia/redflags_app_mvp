from __future__ import annotations

import pandas as pd


def build_final_monitoring_set(
    summary_df: pd.DataFrame,
    flags_df: pd.DataFrame,
    overrides_df: pd.DataFrame,
    month: str,
) -> pd.DataFrame:
    base = summary_df[summary_df["month"] == month].copy()
    if base.empty:
        return base

    auto_keys = set(flags_df[flags_df["month"] == month]["agent_key"].unique().tolist())
    scoped_overrides = overrides_df[overrides_df["report_month"] == month].copy() if not overrides_df.empty else pd.DataFrame()

    include_keys = set(scoped_overrides[scoped_overrides["action_type"] == "include"]["agent_key"].tolist()) if not scoped_overrides.empty else set()
    exclude_keys = set(scoped_overrides[scoped_overrides["action_type"] == "exclude"]["agent_key"].tolist()) if not scoped_overrides.empty else set()

    final_keys = (auto_keys | include_keys) - exclude_keys
    monitored = base[base["agent_key"].isin(final_keys)].copy()

    monitored["auto_flag"] = monitored["agent_key"].isin(auto_keys)
    monitored["manual_include"] = monitored["agent_key"].isin(include_keys)
    monitored["manual_exclude"] = monitored["agent_key"].isin(exclude_keys)
    monitored["inclusion_reason"] = monitored.apply(
        lambda row: "auto red flag + manual include"
        if row["auto_flag"] and row["manual_include"]
        else "auto red flag"
        if row["auto_flag"]
        else "manual include",
        axis=1,
    )
    return monitored
