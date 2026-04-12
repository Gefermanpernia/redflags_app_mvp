from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .normalization import build_agent_key


@dataclass(frozen=True)
class FieldPriority:
    appointments: tuple[str, ...] = ("manual", "excel")
    production: tuple[str, ...] = ("manual", "excel")


def _week_of_month(dates: pd.Series) -> pd.Series:
    return ((dates.dt.day - 1) // 7 + 1).astype(int)


def build_manual_weekly_inputs(facts_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if facts_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    facts = facts_df.copy()
    facts["fact_date"] = pd.to_datetime(facts["fact_date"], errors="coerce")
    facts = facts.dropna(subset=["fact_date"])
    if facts.empty:
        return pd.DataFrame(), pd.DataFrame()

    facts["month"] = facts["fact_date"].dt.to_period("M").astype(str)
    facts["week"] = _week_of_month(facts["fact_date"])
    facts["agent_name"] = facts["agent_name"].astype(str).str.strip()
    facts["hierarchy"] = facts.get("hierarchy", "").fillna("").astype(str).str.strip()
    facts["agent_code"] = facts.get("agent_code", "").fillna("").astype(str).str.strip()
    facts["agent_key"] = facts.apply(
        lambda row: build_agent_key(
            row["agent_name"], row["hierarchy"], agent_code=row.get("agent_code", "")
        ),
        axis=1,
    )

    weekly = (
        facts.groupby(["month", "week", "agent_key"], as_index=False)
        .agg(
            agent_name=("agent_name", "first"),
            hierarchy=("hierarchy", "first"),
            agent_code=("agent_code", "first"),
            appointments=("appointments", "sum"),
            production_weekly=("production", "sum"),
        )
        .sort_values(["month", "agent_key", "week"])
    )

    weekly["production_mtd"] = weekly.groupby(["month", "agent_key"])["production_weekly"].cumsum()

    raw_production = weekly[["month", "week", "agent_name", "hierarchy", "agent_code", "production_mtd"]].copy()
    raw_production["source_sheet"] = "manual_facts"

    raw_appointments = weekly[["month", "week", "agent_name", "hierarchy", "agent_code", "appointments"]].copy()
    raw_appointments["source_sheet"] = "manual_facts"
    return raw_production, raw_appointments


def unify_weekly_sources(
    excel_production: pd.DataFrame,
    excel_appointments: pd.DataFrame,
    manual_facts: pd.DataFrame,
    priority: FieldPriority,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    manual_prod, manual_appt = build_manual_weekly_inputs(manual_facts)

    prod_sources = {
        "excel": excel_production.copy() if not excel_production.empty else pd.DataFrame(),
        "manual": manual_prod,
    }
    appt_sources = {
        "excel": excel_appointments.copy() if not excel_appointments.empty else pd.DataFrame(),
        "manual": manual_appt,
    }

    def _prepare(frame: pd.DataFrame, metric_col: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["month", "week", "agent_name", "hierarchy", "agent_code", metric_col])
        out = frame.copy()
        out["agent_name"] = out["agent_name"].astype(str).str.strip()
        out["hierarchy"] = out.get("hierarchy", "").fillna("")
        if "agent_code" not in out.columns:
            out["agent_code"] = ""
        out["agent_code"] = out["agent_code"].fillna("")
        return out[["month", "week", "agent_name", "hierarchy", "agent_code", metric_col]].copy()

    conflicts: list[dict] = []

    def _select_metric(
        key_cols: list[str],
        metric_col: str,
        order: tuple[str, ...],
        sources: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        prepared = []
        for src_name, src_df in sources.items():
            if src_df.empty:
                continue
            p = _prepare(src_df, metric_col)
            p["source"] = src_name
            prepared.append(p)
        if not prepared:
            return pd.DataFrame(columns=key_cols + [metric_col, "source"])

        stacked = pd.concat(prepared, ignore_index=True)
        rows = []
        for keys, group in stacked.groupby(key_cols, dropna=False):
            selected = None
            values_by_source = {}
            for src in order:
                sample = group[group["source"] == src]
                if sample.empty:
                    continue
                val = float(sample[metric_col].max())
                values_by_source[src] = val
                if selected is None:
                    selected = sample.iloc[0].copy()
                    selected[metric_col] = val
            if selected is None:
                continue
            if len(set(values_by_source.values())) > 1:
                conflicts.append(
                    {
                        "dataset": "datamart",
                        "month": selected["month"],
                        "week": selected["week"],
                        "agent_key": build_agent_key(selected["agent_name"], selected["hierarchy"], agent_code=selected["agent_code"]),
                        "agent_name": selected["agent_name"],
                        "details": f"Conflicto en {metric_col}: {values_by_source}. Prioridad aplicada: {order}.",
                    }
                )
            rows.append(selected)

        return pd.DataFrame(rows)

    key_cols = ["month", "week", "agent_name", "hierarchy", "agent_code"]
    selected_prod = _select_metric(key_cols, "production_mtd", priority.production, prod_sources)
    selected_appt = _select_metric(key_cols, "appointments", priority.appointments, appt_sources)

    if not selected_prod.empty:
        selected_prod["source_sheet"] = selected_prod["source"].apply(lambda s: f"{s}_unified")
    if not selected_appt.empty:
        selected_appt["source_sheet"] = selected_appt["source"].apply(lambda s: f"{s}_unified")

    prod_out = selected_prod[["month", "week", "agent_name", "hierarchy", "agent_code", "production_mtd", "source_sheet"]].copy() if not selected_prod.empty else pd.DataFrame()
    appt_out = selected_appt[["month", "week", "agent_name", "hierarchy", "agent_code", "appointments", "source_sheet"]].copy() if not selected_appt.empty else pd.DataFrame()

    return prod_out, appt_out, pd.DataFrame(conflicts)
