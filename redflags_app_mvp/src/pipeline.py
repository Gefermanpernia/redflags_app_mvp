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
) -> Dict[str, pd.DataFrame]:
    weekly = build_weekly_dataset(raw_production, raw_appointments, config)
    monthly = build_monthly_dataset(weekly)
    flags = evaluate_red_flags(weekly, monthly, config)
    summary = build_summary_table(weekly, flags)

    flagged_agents = summary[summary["active_flags"].astype(str).str.strip() != ""].copy() if not summary.empty else summary
    return {
        "weekly": weekly,
        "monthly": monthly,
        "flags": flags,
        "summary": summary,
        "flagged_agents": flagged_agents,
    }
