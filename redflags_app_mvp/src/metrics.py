from __future__ import annotations

from typing import Iterable, List

import pandas as pd

from .config import ThresholdConfig
from .normalization import normalize_hierarchy, resolve_alias


PRODUCTION_COLUMNS = [
    "month",
    "snapshot_date",
    "week",
    "agent_name",
    "hierarchy",
    "hierarchies_detected",
    "agent_key",
    "production_mtd",
    "production_net_mtd",
    "production_weekly_closed",
    "production_weekly_effective",
    "production_monthly_total",
    "source_sheet",
    "is_completed_week",
]

APPOINTMENT_COLUMNS = [
    "month",
    "week",
    "agent_name",
    "hierarchy",
    "hierarchies_detected",
    "agent_key",
    "appointments",
    "appointments_month_total",
    "source_sheet",
]


def _first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        if str(value).strip() != "":
            return str(value)
    return ""


def _all_unique(values: Iterable[str]) -> str:
    options = sorted({str(v).strip() for v in values if str(v).strip()})
    return ", ".join(options)


def prepare_production_data(
    df: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=PRODUCTION_COLUMNS)

    prod = df.copy()
    prod["agent_name"] = prod["agent_name"].astype(str).str.strip()
    prod["hierarchy"] = prod["hierarchy"].fillna("").astype(str).str.strip()

    prod = (
        prod.groupby(["month", "snapshot_date", "week", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchy", _all_unique),
            production_mtd=("production_mtd", "max"),
            production_net_mtd=("production_net_mtd", "max"),
            source_sheet=("source_sheet", lambda values: ", ".join(sorted(set(map(str, values))))),
        )
        .sort_values(["month", "agent_key", "week", "snapshot_date"])
        .reset_index(drop=True)
    )

    # last snapshot per week defines week state
    prod = prod.groupby(["month", "agent_key", "week"], as_index=False).tail(1)
    prod = prod.sort_values(["month", "agent_key", "week", "snapshot_date"])

    prod["prev_mtd"] = prod.groupby(["month", "agent_key"])["production_mtd"].shift(fill_value=0)
    prod["production_weekly_closed"] = (prod["production_mtd"] - prod["prev_mtd"]).clip(lower=0)
    prod["production_weekly_effective"] = prod["production_weekly_closed"]
    last_week = prod.groupby(["month", "agent_key"])["week"].transform("max")
    prod["is_completed_week"] = prod["week"] < last_week
    if config.include_open_week_as_completed:
        prod["is_completed_week"] = True
    if config.use_open_week_partial:
        prod.loc[~prod["is_completed_week"], "production_weekly_effective"] = prod.loc[
            ~prod["is_completed_week"], "production_mtd"
        ]

    prod["production_monthly_total"] = prod.groupby(["month", "agent_key"])["production_mtd"].transform("max")
    prod["agent_name"] = prod["agent_name"].apply(lambda value: resolve_alias(value, alias_mapping))
    prod["hierarchy"] = prod["hierarchy"].apply(normalize_hierarchy)
    return prod[PRODUCTION_COLUMNS]


def prepare_appointments_data(
    df: pd.DataFrame, alias_mapping: dict[str, str] | None = None
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=APPOINTMENT_COLUMNS)

    appt = df.copy()
    appt["agent_name"] = appt["agent_name"].astype(str).str.strip()
    appt["hierarchy"] = appt["hierarchy"].fillna("").astype(str).str.strip()

    appt = (
        appt.groupby(["month", "week", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchy", _all_unique),
            appointments=("appointments", "max"),
            source_sheet=("source_sheet", lambda values: ", ".join(sorted(set(map(str, values))))),
        )
        .sort_values(["month", "agent_key", "week"])
    )

    appt["appointments_month_total"] = appt.groupby(["month", "agent_key"])["appointments"].transform("sum")
    appt["agent_name"] = appt["agent_name"].apply(lambda value: resolve_alias(value, alias_mapping))
    appt["hierarchy"] = appt["hierarchy"].apply(normalize_hierarchy)
    return appt[APPOINTMENT_COLUMNS]


def build_weekly_dataset(
    production_df: pd.DataFrame,
    appointments_df: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    prod = prepare_production_data(production_df, config, alias_mapping=alias_mapping)
    appt = prepare_appointments_data(appointments_df, alias_mapping=alias_mapping)

    weekly = pd.merge(
        prod,
        appt,
        on=["month", "week", "agent_key"],
        how="outer",
        suffixes=("_prod", "_appt"),
    )

    if weekly.empty:
        return pd.DataFrame(columns=["month", "week", "agent_key", "agent_name", "hierarchy"])

    weekly["agent_name"] = weekly["agent_name_prod"].combine_first(weekly["agent_name_appt"]).fillna("")
    weekly["hierarchy"] = weekly["hierarchy_prod"].combine_first(weekly["hierarchy_appt"]).fillna("")
    weekly["hierarchies_detected"] = (
        weekly["hierarchies_detected_prod"].fillna("") + ", " + weekly["hierarchies_detected_appt"].fillna("")
    ).str.strip(", ")

    numeric_fill_zero = [
        "production_mtd",
        "production_net_mtd",
        "production_weekly_closed",
        "production_weekly_effective",
        "production_monthly_total",
        "appointments",
        "appointments_month_total",
    ]
    for col in numeric_fill_zero:
        if col in weekly.columns:
            weekly[col] = weekly[col].fillna(0.0)

    weekly["source_sheet_prod"] = weekly.get("source_sheet_prod", "").fillna("")
    weekly["source_sheet_appt"] = weekly.get("source_sheet_appt", "").fillna("")
    weekly["is_completed_week"] = weekly.get("is_completed_week", False).fillna(False)

    weekly = weekly[
        [
            "month",
            "snapshot_date",
            "week",
            "agent_key",
            "agent_name",
            "hierarchy",
            "hierarchies_detected",
            "production_mtd",
            "production_net_mtd",
            "production_weekly_closed",
            "production_weekly_effective",
            "production_monthly_total",
            "appointments",
            "appointments_month_total",
            "is_completed_week",
            "source_sheet_prod",
            "source_sheet_appt",
        ]
    ].sort_values(["month", "agent_name", "week"])

    weekly["appointments_month_total"] = weekly.groupby(["month", "agent_key"])["appointments"].transform("sum")
    weekly["production_monthly_total"] = weekly.groupby(["month", "agent_key"])["production_mtd"].transform("max")
    return weekly


def build_monthly_dataset(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "agent_key",
                "agent_name",
                "hierarchy",
                "hierarchies_detected",
                "production_monthly_total",
                "appointments_month_total",
                "weeks_with_activity",
                "last_week",
            ]
        )

    monthly = (
        weekly_df.groupby(["month", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchies_detected", _all_unique),
            production_monthly_total=("production_monthly_total", "max"),
            appointments_month_total=("appointments", "sum"),
            weeks_with_activity=("week", "nunique"),
            last_week=("week", "max"),
        )
        .sort_values(["month", "agent_name"])
    )
    return monthly


def build_summary_table(
    weekly_df: pd.DataFrame,
    flags_df: pd.DataFrame,
    weeks: List[int] | None = None,
) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame()

    weeks = weeks or list(range(1, 6))
    base = (
        weekly_df[["month", "agent_key", "agent_name", "hierarchy", "hierarchies_detected"]]
        .drop_duplicates()
        .copy()
    )

    appointments_pivot = (
        weekly_df.pivot_table(index=["month", "agent_key"], columns="week", values="appointments", aggfunc="sum", fill_value=0)
        .reindex(columns=weeks, fill_value=0)
        .reset_index()
    )
    appointments_pivot.columns = [*appointments_pivot.columns[:2], *[f"appointments_week_{col}" for col in weeks]]

    production_pivot = (
        weekly_df.pivot_table(index=["month", "agent_key"], columns="week", values="production_weekly_closed", aggfunc="sum", fill_value=0)
        .reindex(columns=weeks, fill_value=0)
        .reset_index()
    )
    production_pivot.columns = [*production_pivot.columns[:2], *[f"production_week_{col}" for col in weeks]]

    totals = weekly_df.groupby(["month", "agent_key"], as_index=False).agg(
        appointments_month_total=("appointments", "sum"),
        production_monthly_total=("production_monthly_total", "max"),
        weekly_production_latest=("production_weekly_closed", "last"),
        weekly_appointments_latest=("appointments", "last"),
    )

    summary = base.merge(appointments_pivot, on=["month", "agent_key"], how="left").merge(
        production_pivot, on=["month", "agent_key"], how="left"
    ).merge(totals, on=["month", "agent_key"], how="left")

    if not flags_df.empty:
        flags_agg = (
            flags_df.groupby(["month", "agent_key"], as_index=False)
            .agg(
                active_flags=("flag_id", lambda values: ", ".join(sorted(set(values)))),
                max_severity=("severity", "max"),
                risk_score=("risk_score", "max"),
            )
        )
        summary = summary.merge(flags_agg, on=["month", "agent_key"], how="left")
    else:
        summary["active_flags"] = ""
        summary["max_severity"] = ""
        summary["risk_score"] = 0

    summary["active_flags"] = summary["active_flags"].fillna("")
    summary["max_severity"] = summary["max_severity"].fillna("")
    summary["risk_score"] = summary["risk_score"].fillna(0)
    return summary.sort_values(["month", "risk_score", "production_monthly_total"], ascending=[True, False, False])
