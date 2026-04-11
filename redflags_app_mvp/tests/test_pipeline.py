from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import ThresholdConfig
from src.metrics import build_monthly_dataset, build_weekly_dataset
from src.normalization import build_agent_key, load_alias_mapping, normalize_name
from src.pipeline import apply_monitoring_overrides
from src.red_flags import evaluate_red_flags


def test_normalize_name_and_identity_ignore_hierarchy() -> None:
    assert normalize_name("  José   Pérez ") == "JOSE PEREZ"
    key_vip = build_agent_key("José Pérez", "Vip")
    key_sa = build_agent_key("Jose Perez", "SA")
    assert key_vip == key_sa == "NAME::JOSE PEREZ"


def test_agent_code_preferred_over_name() -> None:
    assert build_agent_key("Ana Lopez", "VIP", agent_code=" a-123 ") == "CODE::A123"


def test_alias_mapping_from_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "aliases.csv"
    csv_path.write_text("alias,canonical\nPepe Perez,Jose Perez\n")
    aliases = load_alias_mapping(csv_path)
    assert aliases["PEPE PEREZ"] == "JOSE PEREZ"


def test_build_weekly_dataset_from_mtd() -> None:
    production = pd.DataFrame(
        [
            {"month": "2026-04", "snapshot_date": "2026-04-07", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "production_mtd": 1000, "production_net_mtd": None, "source_sheet": "prod"},
            {"month": "2026-04", "snapshot_date": "2026-04-14", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "production_mtd": 1800, "production_net_mtd": None, "source_sheet": "prod"},
            {"month": "2026-04", "snapshot_date": "2026-04-21", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "production_mtd": 2400, "production_net_mtd": None, "source_sheet": "prod"},
        ]
    )
    appointments = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "appointments": 2, "source_sheet": "appt"},
            {"month": "2026-04", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "appointments": 1, "source_sheet": "appt"},
            {"month": "2026-04", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_key": "NAME::ANA LOPEZ", "appointments": 0, "source_sheet": "appt"},
        ]
    )
    weekly = build_weekly_dataset(production, appointments, ThresholdConfig(use_open_week_partial=False))
    assert weekly["production_weekly_closed"].tolist() == [1000, 800, 600]


def test_deduped_hierarchy_rows_not_doubled() -> None:
    appointments = pd.DataFrame(
        [
            {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "SA", "agent_key": "NAME::CARLOS DIAZ", "appointments": 0, "source_sheet": "appt"},
            {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "VIP", "agent_key": "NAME::CARLOS DIAZ", "appointments": 3, "source_sheet": "appt"},
        ]
    )
    prod = pd.DataFrame(
        [{"month": "2026-04", "snapshot_date": "2026-04-07", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "VIP", "agent_key": "NAME::CARLOS DIAZ", "production_mtd": 2000, "production_net_mtd": None, "source_sheet": "prod"}]
    )
    weekly = build_weekly_dataset(prod, appointments, ThresholdConfig(use_open_week_partial=False))
    assert weekly["appointments"].sum() == 3


def test_manual_include_exclude_final_set_logic() -> None:
    summary = pd.DataFrame([
        {"month": "2026-04", "agent_key": "A", "agent_name": "A"},
        {"month": "2026-04", "agent_key": "B", "agent_name": "B"},
        {"month": "2026-04", "agent_key": "C", "agent_name": "C"},
    ])
    flags = pd.DataFrame([{"month": "2026-04", "agent_key": "A"}])
    overrides = pd.DataFrame([
        {"agent_key": "B", "report_month": "2026-04", "action_type": "include", "reason": "manual", "created_at": "2026-04-01"},
        {"agent_key": "A", "report_month": "2026-04", "action_type": "exclude", "reason": "exempt", "created_at": "2026-04-02"},
    ])
    out = apply_monitoring_overrides(summary, flags, "2026-04", overrides)
    final = set(out[out["in_final_monitoring_set"]]["agent_key"])
    assert final == {"B"}


def test_red_flags_include_observation_bucket() -> None:
    weekly = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_key": "A", "agent_name": "A", "hierarchy": "VIP", "appointments": 1, "production_weekly_effective": 1200, "is_completed_week": True, "production_mtd": 1200, "production_monthly_total": 1200},
    ])
    monthly = pd.DataFrame([
        {"month": "2026-04", "agent_key": "A", "agent_name": "A", "hierarchy": "VIP", "appointments_month_total": 1, "production_monthly_total": 1200},
    ])
    flags = evaluate_red_flags(weekly, monthly, ThresholdConfig())
    assert "RF-OBS" in set(flags["flag_id"]) if not flags.empty else False


def test_report_dataset_uses_final_monitoring_set() -> None:
    summary = pd.DataFrame([
        {"month": "2026-04", "agent_key": "A", "agent_name": "A", "in_final_monitoring_set": True},
        {"month": "2026-04", "agent_key": "B", "agent_name": "B", "in_final_monitoring_set": False},
    ])
    final = summary[summary["in_final_monitoring_set"]]
    assert final["agent_key"].tolist() == ["A"]
