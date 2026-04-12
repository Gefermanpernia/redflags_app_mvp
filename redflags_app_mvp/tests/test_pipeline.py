from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import ThresholdConfig
from src.data_quality import detect_mixed_months
from src.datamart import FieldPriority, build_manual_weekly_inputs, unify_weekly_sources
from src.metrics import build_monthly_dataset, build_weekly_dataset
from src.monitoring import build_final_monitoring_set
from src.normalization import build_agent_key, load_alias_mapping, normalize_name
from src.parsers import (
    SOURCE_MODE_MONTHLY_AUDIT,
    SOURCE_MODE_WEEKLY_DETAIL,
    filter_frames_by_source_mode,
)
from src.red_flags import compute_risk_score, evaluate_red_flags


def test_normalize_name_matching_without_hierarchy() -> None:
    assert normalize_name("  José   Pérez ") == "jose perez"
    assert build_agent_key("José Pérez", "Vip") == build_agent_key("JOSE PEREZ", "SA")


def test_alias_mapping_from_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "aliases.csv"
    csv_path.write_text("alias,canonical\nPepe Perez,Jose Perez\n")
    aliases = load_alias_mapping(csv_path)
    assert aliases["pepe perez"] == "jose perez"


def test_duplicate_hierarchy_rows_deduped_for_appointments() -> None:
    production = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 2000, "source_sheet": "prod"}
    ])
    appointments = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 0, "source_sheet": "appt"},
        {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "SA", "appointments": 2, "source_sheet": "appt"},
    ])
    weekly, conflicts = build_weekly_dataset(production, appointments, ThresholdConfig())
    assert len(weekly) == 1
    assert weekly.iloc[0]["appointments"] == 2
    assert not conflicts.empty


def test_build_weekly_dataset_from_mtd() -> None:
    production = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 1000, "source_sheet": "prod"},
        {"month": "2026-04", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 1800, "source_sheet": "prod"},
        {"month": "2026-04", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "production_mtd": 2400, "source_sheet": "prod"},
    ])
    appointments = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 2, "source_sheet": "appt"},
        {"month": "2026-04", "week": 2, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 1, "source_sheet": "appt"},
        {"month": "2026-04", "week": 3, "agent_name": "Ana Lopez", "hierarchy": "VIP", "appointments": 0, "source_sheet": "appt"},
    ])
    weekly, _ = build_weekly_dataset(production, appointments, ThresholdConfig(use_open_week_partial=False))
    assert weekly["production_weekly_closed"].tolist() == [1000, 800, 600]


def test_final_monitoring_set_union_minus() -> None:
    summary = pd.DataFrame([
        {"month": "2026-04", "agent_key": "name::ana", "agent_name": "Ana", "hierarchy": "VIP"},
        {"month": "2026-04", "agent_key": "name::bob", "agent_name": "Bob", "hierarchy": "SA"},
        {"month": "2026-04", "agent_key": "name::carla", "agent_name": "Carla", "hierarchy": "VIP"},
    ])
    flags = pd.DataFrame([
        {"month": "2026-04", "agent_key": "name::ana", "flag_id": "RF-001"},
        {"month": "2026-04", "agent_key": "name::bob", "flag_id": "RF-003"},
    ])
    overrides = pd.DataFrame([
        {"report_month": "2026-04", "agent_key": "name::carla", "action_type": "include"},
        {"report_month": "2026-04", "agent_key": "name::bob", "action_type": "exclude"},
    ])
    final = build_final_monitoring_set(summary, flags, overrides, "2026-04")
    assert set(final["agent_key"]) == {"name::ana", "name::carla"}


def test_report_dataset_excludes_manual_exclusions() -> None:
    production = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "production_mtd": 3200, "source_sheet": "prod"},
    ])
    appointments = pd.DataFrame([
        {"month": "2026-04", "week": 1, "agent_name": "Carlos Diaz", "hierarchy": "GERENTE", "appointments": 0, "source_sheet": "appt"},
    ])
    config = ThresholdConfig(use_open_week_partial=False)
    weekly, _ = build_weekly_dataset(production, appointments, config)
    flags = evaluate_red_flags(weekly, build_monthly_dataset(weekly), config)
    assert {"RF-001", "RF-003"}.issubset(set(flags["flag_id"].tolist()))
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


def test_filter_frames_by_source_mode_uses_expected_sheet() -> None:
    frames = {
        "reporte de citas abril": pd.DataFrame([{"x": 1}]),
        "AUDITORIA": pd.DataFrame([{"x": 2}]),
        "OTRA": pd.DataFrame([{"x": 3}]),
    }
    weekly_filtered = filter_frames_by_source_mode(frames, SOURCE_MODE_WEEKLY_DETAIL)
    monthly_filtered = filter_frames_by_source_mode(frames, SOURCE_MODE_MONTHLY_AUDIT)

    assert list(weekly_filtered.keys()) == ["reporte de citas abril"]
    assert list(monthly_filtered.keys()) == ["AUDITORIA"]


def test_build_manual_weekly_inputs_derives_mtd() -> None:
    facts = pd.DataFrame(
        [
            {"fact_date": "2026-04-02", "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "appointments": 1, "production": 300},
            {"fact_date": "2026-04-04", "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "appointments": 1, "production": 200},
            {"fact_date": "2026-04-10", "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "appointments": 2, "production": 400},
        ]
    )
    raw_production, raw_appointments = build_manual_weekly_inputs(facts)
    assert raw_production["production_mtd"].tolist() == [500, 900]
    assert raw_appointments["appointments"].tolist() == [2, 2]


def test_unify_weekly_sources_applies_priority_and_creates_conflict() -> None:
    excel_prod = pd.DataFrame(
        [{"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "production_mtd": 1000, "source_sheet": "prod"}]
    )
    excel_appt = pd.DataFrame(
        [{"month": "2026-04", "week": 1, "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "appointments": 1, "source_sheet": "appt"}]
    )
    manual_facts = pd.DataFrame(
        [{"fact_date": "2026-04-02", "agent_name": "Ana Lopez", "hierarchy": "VIP", "agent_code": "", "appointments": 3, "production": 700}]
    )

    prod, appt, conflicts = unify_weekly_sources(
        excel_prod,
        excel_appt,
        manual_facts,
        priority=FieldPriority(appointments=("manual", "excel"), production=("manual", "excel")),
    )
    assert prod.iloc[0]["production_mtd"] == 700
    assert appt.iloc[0]["appointments"] == 3
    assert not conflicts.empty
