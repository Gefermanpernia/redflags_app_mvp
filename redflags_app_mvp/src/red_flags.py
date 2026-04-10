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
    "risk_score",
    "reason",
    "metrics",
]

SEVERITY_POINTS = {"baja": 5, "media": 12, "media-alta": 18, "alta": 25, "critica": 35}
RULE_POINTS = {"RF-001": 25, "RF-002": 30, "RF-003": 20}


def _severity_points(severity: str) -> int:
    return SEVERITY_POINTS.get(str(severity).strip().lower(), 10)


def compute_risk_score(flag_id: str, severity: str, metrics: Dict[str, Any]) -> int:
    # Fórmula v1.1: score = puntos_regla + puntos_severidad + intensidad_métrica (tope 100)
    base = RULE_POINTS.get(flag_id, 10) + _severity_points(severity)
    intensity = 0
    if flag_id == "RF-001":
        ratio = float(metrics.get("production_monthly_total", 0)) / max(
            float(metrics.get("monthly_threshold", 1)), 1
        )
        intensity = min(int(ratio * 10), 30)
    elif flag_id == "RF-002":
        ratio = float(metrics.get("last_week_production", 0)) / max(
            float(metrics.get("spike_threshold", 1)), 1
        )
        intensity = min(int(ratio * 12), 30)
    elif flag_id == "RF-003":
        ratio = float(metrics.get("weekly_production", 0)) / max(
            float(metrics.get("weekly_threshold", 1)), 1
        )
        intensity = min(int(ratio * 10), 25)
    return max(0, min(100, base + intensity))


def _flag_record(**kwargs: Any) -> Dict[str, Any]:
    metrics = kwargs["metrics"]
    risk_score = compute_risk_score(kwargs["flag_id"], kwargs["severity"], metrics)
    return {
        **kwargs,
        "risk_score": risk_score,
        "metrics": json.dumps(metrics, ensure_ascii=False),
    }


def evaluate_red_flags(
    weekly_df: pd.DataFrame, monthly_df: pd.DataFrame, config: ThresholdConfig
) -> pd.DataFrame:
    flags: List[Dict[str, Any]] = []
    if weekly_df.empty or monthly_df.empty:
        return pd.DataFrame(columns=FLAG_COLUMNS)

    weekly_by_agent = {
        (m, k): g.sort_values("week").copy()
        for (m, k), g in weekly_df.groupby(["month", "agent_key"])
    }

    for _, row in monthly_df.iterrows():
        month, agent_key = row["month"], row["agent_key"]
        agent_name, hierarchy = row["agent_name"], row["hierarchy"]
        appointments_month_total = float(row["appointments_month_total"])
        production_monthly_total = float(row["production_monthly_total"])

        if (
            appointments_month_total == 0
            and production_monthly_total > config.monthly_production_suspicious
        ):
            metrics = {
                "appointments_month_total": appointments_month_total,
                "production_monthly_total": production_monthly_total,
                "monthly_threshold": config.monthly_production_suspicious,
            }
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
                    reason="Producción mensual alta sin citas.",
                    metrics=metrics,
                )
            )

        group = weekly_by_agent[(month, agent_key)]
        if not group.empty:
            last_week = int(group["week"].max())
            last_row = group[group["week"] == last_week].iloc[-1]
            prev_production_total = float(
                group[group["week"] < last_week]["production_weekly_effective"].sum()
            )
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
                metrics = {
                    "last_week": last_week,
                    "last_week_production": last_week_production,
                    "previous_weeks_production_total": prev_production_total,
                    "appointments_month_total": appointments_month_total,
                    "last_week_appointments": last_week_appointments,
                    "spike_threshold": config.spike_last_week_threshold,
                }
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
                        reason="Pico semanal sin actividad previa ni citas.",
                        metrics=metrics,
                    )
                )

    for _, row in weekly_df.iterrows():
        weekly_appointments, weekly_production = (
            float(row["appointments"]),
            float(row["production_weekly_effective"]),
        )
        if (
            weekly_appointments <= config.few_appointments_threshold
            and weekly_production > config.weekly_production_suspicious
        ):
            metrics = {
                "week": int(row["week"]),
                "weekly_appointments": weekly_appointments,
                "weekly_production": weekly_production,
                "weekly_threshold": config.weekly_production_suspicious,
            }
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
                    reason="Alta producción semanal con pocas citas.",
                    metrics=metrics,
                )
            )

    flags_df = pd.DataFrame(flags, columns=FLAG_COLUMNS)
    if flags_df.empty:
        return flags_df
    flags_df = flags_df.drop_duplicates(
        subset=["month", "week", "agent_key", "flag_id", "reason"]
    )
    return flags_df.sort_values(
        ["month", "agent_name", "week", "flag_id"], na_position="last"
    ).reset_index(drop=True)
