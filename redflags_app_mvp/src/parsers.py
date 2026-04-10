from __future__ import annotations

import io
import re
from typing import BinaryIO, Dict, Iterable, List, Mapping

import pandas as pd


def load_excel_sheets(uploaded_file: BinaryIO) -> List[str]:
    uploaded_file.seek(0)
    excel = pd.ExcelFile(uploaded_file)
    return excel.sheet_names


def load_selected_frames(
    uploaded_file: BinaryIO, selected_sheets: Iterable[str]
) -> Dict[str, pd.DataFrame]:
    uploaded_file.seek(0)
    content = uploaded_file.read()
    result: Dict[str, pd.DataFrame] = {}
    for sheet_name in selected_sheets:
        buffer = io.BytesIO(content)
        frame = pd.read_excel(buffer, sheet_name=sheet_name)
        result[sheet_name] = standardize_frame(frame)
    uploaded_file.seek(0)
    return result


def standardize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df.columns = [str(col).strip() for col in df.columns]
    unnamed_mask = pd.Series(df.columns).astype(str).str.startswith("Unnamed")
    if unnamed_mask.any():
        df = df.loc[:, ~unnamed_mask.values]
    return df


def preview_columns(frames: Mapping[str, pd.DataFrame]) -> List[str]:
    columns: List[str] = []
    seen = set()
    for frame in frames.values():
        for col in frame.columns:
            if col not in seen:
                seen.add(col)
                columns.append(col)
    return columns


WEEK_PATTERN = re.compile(r"(\d+)")


def normalize_week(value) -> int | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        week = int(value)
        return week if week > 0 else None
    text = str(value).strip().lower()
    match = WEEK_PATTERN.search(text)
    if match:
        week = int(match.group(1))
        return week if week > 0 else None
    return None


def normalize_month_value(value, fallback_month: str) -> str:
    if (
        value is None
        or (isinstance(value, float) and pd.isna(value))
        or str(value).strip() == ""
    ):
        return fallback_month
    if hasattr(value, "strftime"):
        try:
            return pd.Timestamp(value).strftime("%Y-%m")
        except Exception:
            return str(value)
    return str(value).strip()


def _safe_series(df: pd.DataFrame, column_name: str | None, default="") -> pd.Series:
    if column_name and column_name in df.columns:
        return df[column_name]
    return pd.Series([default] * len(df), index=df.index)


def parse_production_frames(
    frames: Mapping[str, pd.DataFrame],
    mapping: Mapping[str, str],
    layout: str,
    fallback_month: str,
) -> pd.DataFrame:
    parsed: List[pd.DataFrame] = []

    for sheet_name, df in frames.items():
        if layout == "long":
            out = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": _safe_series(df, mapping.get("week")),
                    "production_mtd": pd.to_numeric(
                        _safe_series(df, mapping.get("production_mtd")), errors="coerce"
                    ),
                    "month": _safe_series(
                        df, mapping.get("month"), default=fallback_month
                    ),
                    "source_sheet": sheet_name,
                }
            )
            out["week"] = out["week"].apply(normalize_week)
            out["month"] = out["month"].apply(
                lambda value: normalize_month_value(value, fallback_month)
            )
            out = out.dropna(subset=["production_mtd"])
            out = out[out["agent_name"].astype(str).str.strip() != ""]
            out = out.dropna(subset=["week"])
            out = out.groupby(
                ["source_sheet", "month", "agent_name", "hierarchy", "week"],
                as_index=False,
            )["production_mtd"].max()
            parsed.append(out)
            continue

        wide_rows: List[pd.DataFrame] = []
        for week in range(1, 6):
            column_name = mapping.get(f"mtd_week_{week}")
            if not column_name or column_name not in df.columns:
                continue
            temp = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": week,
                    "production_mtd": pd.to_numeric(
                        _safe_series(df, column_name), errors="coerce"
                    ),
                    "month": _safe_series(
                        df, mapping.get("month"), default=fallback_month
                    ),
                    "source_sheet": sheet_name,
                }
            )
            wide_rows.append(temp)

        if not wide_rows:
            continue

        out = pd.concat(wide_rows, ignore_index=True)
        out["month"] = out["month"].apply(
            lambda value: normalize_month_value(value, fallback_month)
        )
        out = out.dropna(subset=["production_mtd"])
        out = out[out["agent_name"].astype(str).str.strip() != ""]
        out = out.groupby(
            ["source_sheet", "month", "agent_name", "hierarchy", "week"], as_index=False
        )["production_mtd"].max()
        parsed.append(out)

    if not parsed:
        return pd.DataFrame(
            columns=[
                "month",
                "agent_name",
                "hierarchy",
                "week",
                "production_mtd",
                "source_sheet",
            ]
        )

    return pd.concat(parsed, ignore_index=True)


def parse_appointments_frames(
    frames: Mapping[str, pd.DataFrame],
    mapping: Mapping[str, str],
    layout: str,
    fallback_month: str,
) -> pd.DataFrame:
    parsed: List[pd.DataFrame] = []

    for sheet_name, df in frames.items():
        if layout == "long":
            out = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": _safe_series(df, mapping.get("week")),
                    "appointments": pd.to_numeric(
                        _safe_series(df, mapping.get("appointments")), errors="coerce"
                    ),
                    "month": _safe_series(
                        df, mapping.get("month"), default=fallback_month
                    ),
                    "source_sheet": sheet_name,
                }
            )
            out["week"] = out["week"].apply(normalize_week)
            out["month"] = out["month"].apply(
                lambda value: normalize_month_value(value, fallback_month)
            )
            out = out.dropna(subset=["appointments"])
            out = out[out["agent_name"].astype(str).str.strip() != ""]
            out = out.dropna(subset=["week"])
            out = out.groupby(
                ["source_sheet", "month", "agent_name", "hierarchy", "week"],
                as_index=False,
            )["appointments"].sum()
            parsed.append(out)
            continue

        wide_rows: List[pd.DataFrame] = []
        for week in range(1, 6):
            column_name = mapping.get(f"appointments_week_{week}")
            if not column_name or column_name not in df.columns:
                continue
            temp = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": week,
                    "appointments": pd.to_numeric(
                        _safe_series(df, column_name), errors="coerce"
                    ),
                    "month": _safe_series(
                        df, mapping.get("month"), default=fallback_month
                    ),
                    "source_sheet": sheet_name,
                }
            )
            wide_rows.append(temp)

        if not wide_rows:
            continue

        out = pd.concat(wide_rows, ignore_index=True)
        out["month"] = out["month"].apply(
            lambda value: normalize_month_value(value, fallback_month)
        )
        out = out.dropna(subset=["appointments"])
        out = out[out["agent_name"].astype(str).str.strip() != ""]
        out = out.groupby(
            ["source_sheet", "month", "agent_name", "hierarchy", "week"], as_index=False
        )["appointments"].sum()
        parsed.append(out)

    if not parsed:
        return pd.DataFrame(
            columns=[
                "month",
                "agent_name",
                "hierarchy",
                "week",
                "appointments",
                "source_sheet",
            ]
        )

    return pd.concat(parsed, ignore_index=True)
