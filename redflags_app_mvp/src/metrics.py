from __future__ import annotations

from typing import Iterable, List, Tuple

import pandas as pd

from .config import ThresholdConfig
from .normalization import build_agent_key, normalize_hierarchy, resolve_alias


PRODUCTION_COLUMNS = [
    "month",
    "week",
    "snapshot_date",
    "agent_name",
    "hierarchy",
    "hierarchies_detected",
    "agent_code",
    "agent_key",
    "production_mtd",
    "production_weekly_closed",
    "production_weekly_effective",
    "production_monthly_total",
    "is_completed_week",
    "source_sheet",
]

APPOINTMENT_COLUMNS = [
    "month",
    "week",
    "agent_name",
    "hierarchy",
    "hierarchies_detected",
    "agent_code",
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


def _join_unique(values: Iterable[str]) -> str:
    clean = sorted({str(v).strip() for v in values if str(v).strip()})
    return ", ".join(clean)


def _build_conflict(kind: str, row: pd.Series, details: str) -> dict:
    return {
        "dataset": kind,
        "month": row.get("month"),
        "week": row.get("week"),
        "snapshot_date": row.get("snapshot_date", ""),
        "agent_key": row.get("agent_key"),
        "agent_name": row.get("agent_name"),
        "details": details,
    }


def prepare_production_data(
    df: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(columns=PRODUCTION_COLUMNS), pd.DataFrame(columns=["dataset", "month", "week", "snapshot_date", "agent_key", "agent_name", "details"])

    prod = df.copy()
    prod["agent_name"] = prod["agent_name"].astype(str).str.strip()
    prod["hierarchy"] = prod.get("hierarchy", "").fillna("").astype(str).str.strip()
    if "agent_code" not in prod.columns:
        prod["agent_code"] = ""
    prod["agent_code"] = prod["agent_code"].fillna("").astype(str).str.strip()
    if "snapshot_date" not in prod.columns:
        prod["snapshot_date"] = pd.NaT
    prod["snapshot_date"] = pd.to_datetime(prod["snapshot_date"], errors="coerce")
    prod["agent_key"] = prod.apply(
        lambda row: build_agent_key(
            row["agent_name"],
            row["hierarchy"],
            alias_mapping=alias_mapping,
            agent_code=row.get("agent_code"),
        ),
        axis=1,
    )

    conflict_rows = []
    deduped_rows = []
    key_cols = ["month", "week", "agent_key", "snapshot_date"]
    for _, group in prod.groupby(key_cols, dropna=False):
        group = group.copy()
        if len(group) == 1:
            deduped_rows.append(group.iloc[0])
            continue

        gross_values = set(group["production_mtd"].dropna().astype(float).round(2).tolist())
        net_present = "production_net" in group.columns and group["production_net"].notna().any()
        if len(gross_values) <= 1:
            if "production_net" in group.columns:
                group = group.assign(_net_rank=group["production_net"].notna().astype(int))
                deduped_rows.append(group.sort_values(["_net_rank"], ascending=False).iloc[0].drop(labels=["_net_rank"]))
            else:
                deduped_rows.append(group.iloc[0])
        else:
            selected = group.sort_values("production_mtd", ascending=False).iloc[0]
            deduped_rows.append(selected)
            conflict_rows.append(
                _build_conflict(
                    "production",
                    selected,
                    f"Valores MTD conflictivos detectados {sorted(gross_values)}. Se seleccionó el mayor de forma determinista.",
                )
            )

    prod = pd.DataFrame(deduped_rows)
    prod = (
        prod.groupby(["month", "week", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchy", _join_unique),
            agent_code=("agent_code", _first_non_empty),
            snapshot_date=("snapshot_date", "max"),
            production_mtd=("production_mtd", "max"),
            source_sheet=("source_sheet", _join_unique),
        )
        .sort_values(["month", "agent_key", "week"])
        .reset_index(drop=True)
    )

    prod["prev_mtd"] = prod.groupby(["month", "agent_key"])["production_mtd"].shift(fill_value=0)
    prod["production_weekly_closed"] = (prod["production_mtd"] - prod["prev_mtd"]).clip(lower=0)
    prod["production_weekly_effective"] = prod["production_weekly_closed"]
    max_week = prod.groupby(["month", "agent_key"])["week"].transform("max")
    prod["is_completed_week"] = prod["week"] < max_week
    if config.use_open_week_partial:
        prod.loc[prod["week"] == max_week, "production_weekly_effective"] = prod.loc[
            prod["week"] == max_week, "production_weekly_closed"
        ]
    prod["production_monthly_total"] = prod.groupby(["month", "agent_key"])["production_mtd"].transform("max")

    prod["agent_name"] = prod["agent_name"].apply(lambda value: resolve_alias(value, alias_mapping)).str.title()
    prod["hierarchy"] = prod["hierarchy"].apply(normalize_hierarchy)

    conflicts_df = pd.DataFrame(conflict_rows)
    return prod[PRODUCTION_COLUMNS], conflicts_df


def prepare_appointments_data(
    df: pd.DataFrame, alias_mapping: dict[str, str] | None = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(columns=APPOINTMENT_COLUMNS), pd.DataFrame(columns=["dataset", "month", "week", "snapshot_date", "agent_key", "agent_name", "details"])

    appt = df.copy()
    appt["agent_name"] = appt["agent_name"].astype(str).str.strip()
    appt["hierarchy"] = appt.get("hierarchy", "").fillna("").astype(str).str.strip()
    if "agent_code" not in appt.columns:
        appt["agent_code"] = ""
    appt["agent_code"] = appt["agent_code"].fillna("").astype(str).str.strip()
    appt["agent_key"] = appt.apply(
        lambda row: build_agent_key(
            row["agent_name"],
            row["hierarchy"],
            alias_mapping=alias_mapping,
            agent_code=row.get("agent_code"),
        ),
        axis=1,
    )

    conflict_rows = []
    deduped_rows = []
    for _, group in appt.groupby(["month", "week", "agent_key"], dropna=False):
        if len(group) == 1:
            deduped_rows.append(group.iloc[0])
            continue
        values = group["appointments"].dropna().astype(float).tolist()
        non_null_values = set(values)
        if not values:
            deduped_rows.append(group.iloc[0])
        elif len(non_null_values) == 1:
            deduped_rows.append(group[group["appointments"].notna()].iloc[0])
        else:
            winner = max(non_null_values)
            selected = group[group["appointments"] == winner].iloc[0]
            deduped_rows.append(selected)
            conflict_rows.append(
                _build_conflict(
                    "appointments",
                    selected,
                    f"Citas conflictivas {sorted(non_null_values)}. Se seleccionó el máximo ({winner}).",
                )
            )

    appt = pd.DataFrame(deduped_rows)
    appt = (
        appt.groupby(["month", "week", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchy", _join_unique),
            agent_code=("agent_code", _first_non_empty),
            appointments=("appointments", "max"),
            source_sheet=("source_sheet", _join_unique),
        )
        .sort_values(["month", "agent_key", "week"])
    )

    appt["appointments_month_total"] = appt.groupby(["month", "agent_key"])["appointments"].transform("sum")
    appt["agent_name"] = appt["agent_name"].apply(lambda value: resolve_alias(value, alias_mapping)).str.title()
    appt["hierarchy"] = appt["hierarchy"].apply(normalize_hierarchy)
    conflicts_df = pd.DataFrame(conflict_rows)
    return appt[APPOINTMENT_COLUMNS], conflicts_df


def build_weekly_dataset(
    production_df: pd.DataFrame,
    appointments_df: pd.DataFrame,
    config: ThresholdConfig,
    alias_mapping: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    prod, prod_conflicts = prepare_production_data(production_df, config, alias_mapping=alias_mapping)
    appt, appt_conflicts = prepare_appointments_data(appointments_df, alias_mapping=alias_mapping)

    weekly = pd.merge(
        prod,
        appt,
        on=["month", "week", "agent_key"],
        how="outer",
        suffixes=("_prod", "_appt"),
    )

    if weekly.empty:
        return pd.DataFrame(), pd.concat([prod_conflicts, appt_conflicts], ignore_index=True)

    weekly["agent_name"] = weekly["agent_name_prod"].combine_first(weekly["agent_name_appt"]).fillna("")
    weekly["hierarchy"] = weekly["hierarchy_prod"].combine_first(weekly["hierarchy_appt"]).fillna("")
    weekly["hierarchies_detected"] = weekly["hierarchies_detected_prod"].combine_first(weekly["hierarchies_detected_appt"]).fillna("")

    numeric_fill_zero = [
        "production_mtd",
        "production_weekly_closed",
        "production_weekly_effective",
        "production_monthly_total",
        "appointments",
        "appointments_month_total",
    ]
    for col in numeric_fill_zero:
        if col in weekly.columns:
            weekly[col] = weekly[col].fillna(0.0)

    weekly = weekly[[
        "month",
        "week",
        "agent_key",
        "agent_name",
        "hierarchy",
        "hierarchies_detected",
        "production_mtd",
        "production_weekly_closed",
        "production_weekly_effective",
        "production_monthly_total",
        "appointments",
        "appointments_month_total",
        "is_completed_week",
        "snapshot_date",
        "source_sheet_prod",
        "source_sheet_appt",
    ]].sort_values(["month", "agent_name", "week"])

    weekly["appointments_month_total"] = weekly.groupby(["month", "agent_key"])["appointments"].transform("sum")
    weekly["production_monthly_total"] = weekly.groupby(["month", "agent_key"])["production_mtd"].transform("max")
    conflicts = pd.concat([prod_conflicts, appt_conflicts], ignore_index=True)
    return weekly, conflicts


def build_monthly_dataset(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame(columns=["month", "agent_key", "agent_name", "hierarchy", "hierarchies_detected", "production_monthly_total", "appointments_month_total", "weeks_with_activity", "last_week"])

    monthly = (
        weekly_df.groupby(["month", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", _first_non_empty),
            hierarchy=("hierarchy", _first_non_empty),
            hierarchies_detected=("hierarchies_detected", _join_unique),
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
    base = weekly_df[["month", "agent_key", "agent_name", "hierarchy", "hierarchies_detected"]].drop_duplicates().copy()

    appointments_pivot = (
        weekly_df.pivot_table(index=["month", "agent_key"], columns="week", values="appointments", aggfunc="sum", fill_value=0)
        .reindex(columns=weeks, fill_value=0)
        .reset_index()
    )
    appointments_pivot.columns = [*appointments_pivot.columns[:2], *[f"appointments_week_{col}" for col in weeks]]

    production_pivot = (
        weekly_df.pivot_table(index=["month", "agent_key"], columns="week", values="production_weekly_effective", aggfunc="sum", fill_value=0)
        .reindex(columns=weeks, fill_value=0)
        .reset_index()
    )
    production_pivot.columns = [*production_pivot.columns[:2], *[f"production_week_{col}" for col in weeks]]

    monthly = weekly_df.groupby(["month", "agent_key"], as_index=False).agg(
        production_monthly_total=("production_monthly_total", "max"),
        appointments_month_total=("appointments", "sum"),
    )

    summary = base.merge(monthly, on=["month", "agent_key"], how="left")
    summary = summary.merge(appointments_pivot, on=["month", "agent_key"], how="left")
    summary = summary.merge(production_pivot, on=["month", "agent_key"], how="left")

    if flags_df.empty:
        summary["active_flags"] = ""
        summary["risk_score"] = 0
        summary["severity"] = ""
        return summary

    by_agent = flags_df.groupby(["month", "agent_key"], as_index=False).agg(
        active_flags=("flag_id", lambda s: ", ".join(sorted(set(s.astype(str))))),
        risk_score=("risk_score", "max"),
        severity=("severity", lambda s: _first_non_empty(sorted(set(s.astype(str))))),
    )
    summary = summary.merge(by_agent, on=["month", "agent_key"], how="left")
    summary["active_flags"] = summary["active_flags"].fillna("")
    summary["risk_score"] = summary["risk_score"].fillna(0)
    summary["severity"] = summary["severity"].fillna("")
    return summary
