from __future__ import annotations

from typing import Mapping

import pandas as pd


def validate_sheet_columns(
    frames: Mapping[str, pd.DataFrame],
    mapping: Mapping[str, str],
    required_fields: list[str],
    dataset_name: str,
) -> list[str]:
    errors: list[str] = []
    for field in required_fields:
        col = mapping.get(field)
        if not col:
            errors.append(f"{dataset_name}: campo obligatorio sin mapear: {field}")
            continue
        for sheet_name, frame in frames.items():
            if col not in frame.columns:
                errors.append(
                    f"{dataset_name} / hoja '{sheet_name}': falta columna '{col}' (campo {field})"
                )
    return errors


def detect_mixed_months(df: pd.DataFrame, dataset_name: str) -> list[str]:
    errors: list[str] = []
    if df.empty or "source_sheet" not in df.columns:
        return errors
    for sheet_name, group in df.groupby("source_sheet"):
        months = sorted(set(group["month"].dropna().astype(str).str.strip().tolist()))
        if len(months) > 1:
            errors.append(
                f"{dataset_name} / hoja '{sheet_name}' contiene meses mezclados: {', '.join(months)}"
            )
    return errors


def build_quality_summary(
    raw_production: pd.DataFrame, raw_appointments: pd.DataFrame
) -> pd.DataFrame:
    rows = []
    for name, df in (("produccion", raw_production), ("citas", raw_appointments)):
        rows.append(
            {
                "dataset": name,
                "rows": len(df),
                "sheets": df["source_sheet"].nunique()
                if not df.empty and "source_sheet" in df.columns
                else 0,
                "months": ", ".join(sorted(set(df["month"].astype(str).tolist())))
                if not df.empty
                else "",
                "agents": df["agent_name"].nunique()
                if not df.empty and "agent_name" in df.columns
                else 0,
            }
        )
    return pd.DataFrame(rows)
