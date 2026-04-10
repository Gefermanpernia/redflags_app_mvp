from __future__ import annotations

import pandas as pd

from src.config import ThresholdConfig
from src.metrics import build_monthly_dataset, build_weekly_dataset
from src.normalization import build_agent_key, normalize_name
from src.red_flags import evaluate_red_flags



def test_normalize_name_removes_accents_and_spaces() -> None:
    assert normalize_name("  José   Pérez ") == "JOSE PEREZ"
    assert build_agent_key("José Pérez", "Vip") == "JOSE PEREZ::VIP"



def test_build_weekly_dataset_from_mtd() -> None:
    production = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 1000, "source_sheet": "prod"},
            {"month": "2026-04", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 1800, "source_sheet": "prod"},
            {"month": "2026-04", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 2400, "source_sheet": "prod"},
        ]
    )
    appointments = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 2, "source_sheet": "appt"},
            {"month": "2026-04", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 1, "source_sheet": "appt"},
            {"month": "2026-04", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 0, "source_sheet": "appt"},
        ]
    )

    weekly = build_weekly_dataset(production, appointments, ThresholdConfig(use_open_week_partial=False))
    closed = weekly["production_weekly_closed"].tolist()
    assert closed == [1000, 800, 600]
    assert weekly["production_monthly_total"].max() == 2400
    assert weekly["appointments_month_total"].max() == 3



def test_red_flags_detect_expected_rules() -> None:
    production = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "production_mtd": 0, "source_sheet": "prod"},
            {"month": "2026-04", "week": 2, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "production_mtd": 0, "source_sheet": "prod"},
            {"month": "2026-04", "week": 3, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "production_mtd": 3200, "source_sheet": "prod"},
            {"month": "2026-04", "week": 1, "agent_name": "Luz Mora", "hierarchy": "VIP", "production_mtd": 1600, "source_sheet": "prod"},
        ]
    )
    appointments = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "appointments": 0, "source_sheet": "appt"},
            {"month": "2026-04", "week": 2, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "appointments": 0, "source_sheet": "appt"},
            {"month": "2026-04", "week": 3, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "appointments": 0, "source_sheet": "appt"},
            {"month": "2026-04", "week": 1, "agent_name": "Luz Mora", "hierarchy": "VIP", "appointments": 0, "source_sheet": "appt"},
        ]
    )

    config = ThresholdConfig(use_open_week_partial=False)
    weekly = build_weekly_dataset(production, appointments, config)
    monthly = build_monthly_dataset(weekly)
    flags = evaluate_red_flags(weekly, monthly, config)

    flag_ids = set(flags["flag_id"].tolist())
    assert "RF-001" in flag_ids
    assert "RF-002" in flag_ids
    assert "RF-003" in flag_ids
