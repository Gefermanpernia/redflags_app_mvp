from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import ThresholdConfig
from src.data_quality import detect_mixed_months
from src.metrics import build_monthly_dataset, build_weekly_dataset
from src.normalization import build_agent_key, load_alias_mapping, normalize_name
from src.red_flags import compute_risk_score, evaluate_red_flags


def test_normalize_name_removes_accents_and_spaces() -> None:
    assert normalize_name("  José   Pérez ") == "JOSE PEREZ"
    assert build_agent_key("José Pérez", "Vip") == "JOSE PEREZ::VIP"


def test_alias_mapping_from_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "aliases.csv"
    csv_path.write_text("alias,canonical\nPepe Perez,Jose Perez\n")
    aliases = load_alias_mapping(csv_path)
    assert aliases["PEPE PEREZ"] == "JOSE PEREZ"
    assert (
        build_agent_key("Pepe Pérez", "VIP", alias_mapping=aliases) == "JOSE PEREZ::VIP"
    )


def test_build_weekly_dataset_from_mtd() -> None:
    production = pd.DataFrame(
        [
            {
                "month": "2026-04",
                "week": 1,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "production_mtd": 1000,
                "source_sheet": "prod",
            },
            {
                "month": "2026-04",
                "week": 2,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "production_mtd": 1800,
                "source_sheet": "prod",
            },
            {
                "month": "2026-04",
                "week": 3,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "production_mtd": 2400,
                "source_sheet": "prod",
            },
        ]
    )
    appointments = pd.DataFrame(
        [
            {
                "month": "2026-04",
                "week": 1,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "appointments": 2,
                "source_sheet": "appt",
            },
            {
                "month": "2026-04",
                "week": 2,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "appointments": 1,
                "source_sheet": "appt",
            },
            {
                "month": "2026-04",
                "week": 3,
                "agent_name": "Ana Lopez",
                "hierarchy": "VIP",
                "appointments": 0,
                "source_sheet": "appt",
            },
        ]
    )
    weekly = build_weekly_dataset(
        production, appointments, ThresholdConfig(use_open_week_partial=False)
    )
    assert weekly["production_weekly_closed"].tolist() == [1000, 800, 600]


def test_red_flags_detect_expected_rules_and_risk() -> None:
    production = pd.DataFrame(
        [
            {
                "month": "2026-04",
                "week": 1,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "production_mtd": 0,
                "source_sheet": "prod",
            },
            {
                "month": "2026-04",
                "week": 2,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "production_mtd": 0,
                "source_sheet": "prod",
            },
            {
                "month": "2026-04",
                "week": 3,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "production_mtd": 3200,
                "source_sheet": "prod",
            },
        ]
    )
    appointments = pd.DataFrame(
        [
            {
                "month": "2026-04",
                "week": 1,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "appointments": 0,
                "source_sheet": "appt",
            },
            {
                "month": "2026-04",
                "week": 2,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "appointments": 0,
                "source_sheet": "appt",
            },
            {
                "month": "2026-04",
                "week": 3,
                "agent_name": "Carlos Diaz",
                "hierarchy": "GERENTE",
                "appointments": 0,
                "source_sheet": "appt",
            },
        ]
    )
    config = ThresholdConfig(use_open_week_partial=False)
    weekly = build_weekly_dataset(production, appointments, config)
    flags = evaluate_red_flags(weekly, build_monthly_dataset(weekly), config)
    assert {"RF-001", "RF-002", "RF-003"}.issubset(set(flags["flag_id"].tolist()))
    assert flags["risk_score"].max() <= 100
    assert (
        compute_risk_score(
            "RF-001",
            "alta",
            {"production_monthly_total": 3000, "monthly_threshold": 1500},
        )
        > 0
    )


def test_detect_mixed_months_by_sheet() -> None:
    frame = pd.DataFrame(
        [
            {"source_sheet": "S1", "month": "2026-03", "agent_name": "A"},
            {"source_sheet": "S1", "month": "2026-04", "agent_name": "B"},
        ]
    )
    errors = detect_mixed_months(frame, "Producción")
    assert errors
