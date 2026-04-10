from __future__ import annotations

import json
from typing import Any, Dict, List

import pandas as pd

from .config import ThresholdConfig


FLAG_COLUMNS = [
    "month",
    "week",
    "agent_key",
    "agent_name",
    "hierarchy",
    "flag_id",
    "flag_name",
    "scope",
    "severity",
    "reason",
    "metrics",
]



def _flag_record(
    *,
    month: str,
    week: int | None,
    agent_key: str,
    agent_name: str,
    hierarchy: str,
    flag_id: str,
    flag_name: str,
    scope: str,
    severity: str,
    reason: str,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "month": month,
        "week": week,
        "agent_key": agent_key,
        "agent_name": agent_name,
        "hierarchy": hierarchy,
        "flag_id": flag_id,
        "flag_name": flag_name,
        "scope": scope,
        "severity": severity,
        "reason": reason,
        "metrics": json.dumps(metrics, ensure_ascii=False),
    }



def evaluate_red_flags(
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    config: ThresholdConfig,
) -> pd.DataFrame:
    flags: List[Dict[str, Any]] = []

    if weekly_df.empty or monthly_df.empty:
        return pd.DataFrame(columns=FLAG_COLUMNS)

    weekly_by_agent = {
        (month, agent_key): group.sort_values("week").copy()
        for (month, agent_key), group in weekly_df.groupby(["month", "agent_key"])
    }

    for _, row in monthly_df.iterrows():
        month = row["month"]
        agent_key = row["agent_key"]
        agent_name = row["agent_name"]
        hierarchy = row["hierarchy"]
        appointments_month_total = float(row["appointments_month_total"])
        production_monthly_total = float(row["production_monthly_total"])

        # Regla A
        if appointments_month_total == 0 and production_monthly_total > config.monthly_production_suspicious:
            flags.append(
                _flag_record(
                    month=month,
                    week=None,
                    agent_key=agent_key,
                    agent_name=agent_name,
                    hierarchy=hierarchy,
                    flag_id="RF-001",
                    flag_name="Sin citas y alta producción mensual",
                    scope="mensual",
                    severity=config.severity_rule_a,
                    reason=(
                        f"Producción mensual {production_monthly_total:,.2f} con 0 citas en el mes. "
                        f"Supera el umbral de {config.monthly_production_suspicious:,.2f}."
                    ),
                    metrics={
                        "appointments_month_total": appointments_month_total,
                        "production_monthly_total": production_monthly_total,
                        "monthly_threshold": config.monthly_production_suspicious,
                    },
                )
            )

        # Regla B
        group = weekly_by_agent[(month, agent_key)]
        if not group.empty:
            last_week = int(group["week"].max())
            last_row = group[group["week"] == last_week].iloc[-1]
            prev_rows = group[group["week"] < last_week]
            prev_production_total = float(prev_rows["production_weekly_effective"].sum())
            last_week_production = float(last_row["production_weekly_effective"])
            last_week_appointments = float(last_row["appointments"])
            low_appointments = (
                appointments_month_total <= config.few_appointments_threshold
                or last_week_appointments <= config.few_appointments_threshold
            )
            if (
                last_week_production >= config.spike_last_week_threshold
                and prev_production_total <= config.insignificant_production_threshold
                and low_appointments
            ):
                flags.append(
                    _flag_record(
                        month=month,
                        week=last_week,
                        agent_key=agent_key,
                        agent_name=agent_name,
                        hierarchy=hierarchy,
                        flag_id="RF-002",
                        flag_name="Pico en última semana sin actividad previa",
                        scope="semanal",
                        severity=config.severity_rule_b,
                        reason=(
                            f"Semana {last_week} con producción {last_week_production:,.2f}, "
                            f"mientras semanas previas suman {prev_production_total:,.2f} y las citas son bajas o nulas."
                        ),
                        metrics={
                            "last_week": last_week,
                            "last_week_production": last_week_production,
                            "previous_weeks_production_total": prev_production_total,
                            "appointments_month_total": appointments_month_total,
                            "last_week_appointments": last_week_appointments,
                            "spike_threshold": config.spike_last_week_threshold,
                            "insignificant_production_threshold": config.insignificant_production_threshold,
                            "few_appointments_threshold": config.few_appointments_threshold,
                        },
                    )
                )

    # Regla C
    for _, row in weekly_df.iterrows():
        weekly_appointments = float(row["appointments"])
        weekly_production = float(row["production_weekly_effective"])
        if (
            weekly_appointments <= config.few_appointments_threshold
            and weekly_production > config.weekly_production_suspicious
        ):
            flags.append(
                _flag_record(
                    month=row["month"],
                    week=int(row["week"]),
                    agent_key=row["agent_key"],
                    agent_name=row["agent_name"],
                    hierarchy=row["hierarchy"],
                    flag_id="RF-003",
                    flag_name="Pocas o cero citas con alta producción semanal",
                    scope="semanal",
                    severity=config.severity_rule_c,
                    reason=(
                        f"Semana {int(row['week'])} con {weekly_appointments:,.0f} citas y producción de "
                        f"{weekly_production:,.2f}, por encima de {config.weekly_production_suspicious:,.2f}."
                    ),
                    metrics={
                        "week": int(row["week"]),
                        "weekly_appointments": weekly_appointments,
                        "weekly_production": weekly_production,
                        "few_appointments_threshold": config.few_appointments_threshold,
                        "weekly_threshold": config.weekly_production_suspicious,
                    },
                )
            )

    flags_df = pd.DataFrame(flags, columns=FLAG_COLUMNS)
    if flags_df.empty:
        return flags_df

    flags_df = flags_df.drop_duplicates(subset=["month", "week", "agent_key", "flag_id", "reason"])
    return flags_df.sort_values(["month", "agent_name", "week", "flag_id"], na_position="last").reset_index(drop=True)
