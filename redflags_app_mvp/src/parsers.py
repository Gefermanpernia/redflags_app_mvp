from __future__ import annotations

import io
import re
from typing import BinaryIO, Dict, Iterable, List, Mapping

import pandas as pd

from .normalization import build_agent_key, normalize_hierarchy


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


def normalize_date_value(value, fallback_date: str) -> str:
    if (
        value is None
        or (isinstance(value, float) and pd.isna(value))
        or str(value).strip() == ""
    ):
        return fallback_date
    if hasattr(value, "strftime"):
        try:
            return pd.Timestamp(value).strftime("%Y-%m-%d")
        except Exception:
            return str(value)
    return str(value).strip()


def _safe_series(df: pd.DataFrame, column_name: str | None, default="") -> pd.Series:
    if column_name and column_name in df.columns:
        return df[column_name]
    return pd.Series([default] * len(df), index=df.index)


def _resolve_production_duplicates(out: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    deduped_rows: list[dict] = []
    conflicts: list[dict] = []
    group_cols = ["month", "snapshot_date", "agent_key", "week"]
    for _, group in out.groupby(group_cols):
        gross_unique = {float(v) for v in group["production_mtd"].dropna().tolist()}
        net_unique = {float(v) for v in group["production_net_mtd"].dropna().tolist()}
        if len(gross_unique) > 1 or len(net_unique) > 1:
            candidate = group.sort_values(["production_net_mtd", "production_mtd"], ascending=False).iloc[0]
            conflicts.append(
                {
                    **{k: candidate[k] for k in group_cols},
                    "dataset": "production",
                    "issue": "conflicting_duplicate_values",
                    "values": group[["source_sheet", "production_mtd", "production_net_mtd"]].to_dict("records"),
                }
            )
            deduped_rows.append(candidate.to_dict())
            continue
        best_idx = group["production_net_mtd"].notna().astype(int).idxmax()
        deduped_rows.append(group.loc[best_idx].to_dict())
    return pd.DataFrame(deduped_rows), pd.DataFrame(conflicts)


def _resolve_appointment_duplicates(out: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    deduped_rows: list[dict] = []
    conflicts: list[dict] = []
    group_cols = ["month", "week", "agent_key"]
    for _, group in out.groupby(group_cols):
        non_null = group["appointments"].dropna()
        if non_null.empty:
            chosen = group.iloc[0]
        elif non_null.nunique() == 1:
            chosen = group.loc[non_null.index[0]]
        else:
            max_idx = group["appointments"].idxmax()
            chosen = group.loc[max_idx]
            conflicts.append(
                {
                    **{k: chosen[k] for k in group_cols},
                    "dataset": "appointments",
                    "issue": "conflicting_duplicate_values",
                    "values": group[["source_sheet", "hierarchy", "appointments"]].to_dict("records"),
                    "chosen_policy": "max_appointments",
                }
            )
        deduped_rows.append(chosen.to_dict())
    return pd.DataFrame(deduped_rows), pd.DataFrame(conflicts)


def parse_production_frames(
    frames: Mapping[str, pd.DataFrame],
    mapping: Mapping[str, str],
    layout: str,
    fallback_month: str,
    fallback_snapshot_date: str,
    alias_mapping: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed: List[pd.DataFrame] = []

    for sheet_name, df in frames.items():
        if layout == "long":
            out = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "agent_code": _safe_series(df, mapping.get("agent_code"), default=""),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": _safe_series(df, mapping.get("week")),
                    "production_mtd": pd.to_numeric(
                        _safe_series(df, mapping.get("production_mtd")), errors="coerce"
                    ),
                    "production_net_mtd": pd.to_numeric(
                        _safe_series(df, mapping.get("production_net_mtd")), errors="coerce"
                    ),
                    "month": _safe_series(df, mapping.get("month"), default=fallback_month),
                    "snapshot_date": _safe_series(df, mapping.get("snapshot_date"), default=fallback_snapshot_date),
                    "source_sheet": sheet_name,
                }
            )
            out["week"] = out["week"].apply(normalize_week)
        else:
            wide_rows: List[pd.DataFrame] = []
            for week in range(1, 6):
                column_name = mapping.get(f"mtd_week_{week}")
                if not column_name or column_name not in df.columns:
                    continue
                temp = pd.DataFrame(
                    {
                        "agent_name": _safe_series(df, mapping.get("agent_name")),
                        "agent_code": _safe_series(df, mapping.get("agent_code"), default=""),
                        "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                        "week": week,
                        "production_mtd": pd.to_numeric(_safe_series(df, column_name), errors="coerce"),
                        "production_net_mtd": pd.to_numeric(_safe_series(df, mapping.get("production_net_mtd")), errors="coerce"),
                        "month": _safe_series(df, mapping.get("month"), default=fallback_month),
                        "snapshot_date": _safe_series(df, mapping.get("snapshot_date"), default=fallback_snapshot_date),
                        "source_sheet": sheet_name,
                    }
                )
                wide_rows.append(temp)
            if not wide_rows:
                continue
            out = pd.concat(wide_rows, ignore_index=True)

        out["month"] = out["month"].apply(lambda value: normalize_month_value(value, fallback_month))
        out["snapshot_date"] = out["snapshot_date"].apply(lambda v: normalize_date_value(v, fallback_snapshot_date))
        out = out.dropna(subset=["production_mtd"])
        out = out[out["agent_name"].astype(str).str.strip() != ""]
        out = out.dropna(subset=["week"])
        out["hierarchy"] = out["hierarchy"].apply(normalize_hierarchy)
        out["agent_key"] = out.apply(
            lambda row: build_agent_key(row["agent_name"], row["hierarchy"], alias_mapping=alias_mapping, agent_code=row["agent_code"]),
            axis=1,
        )
        parsed.append(out)

    if not parsed:
        empty = pd.DataFrame(columns=["month", "snapshot_date", "agent_name", "agent_code", "hierarchy", "week", "production_mtd", "production_net_mtd", "source_sheet", "agent_key"])
        return empty, pd.DataFrame(columns=["dataset", "issue", "values"])

    combined = pd.concat(parsed, ignore_index=True)
    deduped, conflicts = _resolve_production_duplicates(combined)
    return deduped.reset_index(drop=True), conflicts


def parse_appointments_frames(
    frames: Mapping[str, pd.DataFrame],
    mapping: Mapping[str, str],
    layout: str,
    fallback_month: str,
    alias_mapping: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed: List[pd.DataFrame] = []

    for sheet_name, df in frames.items():
        if layout == "long":
            out = pd.DataFrame(
                {
                    "agent_name": _safe_series(df, mapping.get("agent_name")),
                    "agent_code": _safe_series(df, mapping.get("agent_code"), default=""),
                    "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                    "week": _safe_series(df, mapping.get("week")),
                    "appointments": pd.to_numeric(_safe_series(df, mapping.get("appointments")), errors="coerce"),
                    "month": _safe_series(df, mapping.get("month"), default=fallback_month),
                    "source_sheet": sheet_name,
                }
            )
            out["week"] = out["week"].apply(normalize_week)
        else:
            wide_rows: List[pd.DataFrame] = []
            for week in range(1, 6):
                column_name = mapping.get(f"appointments_week_{week}")
                if not column_name or column_name not in df.columns:
                    continue
                temp = pd.DataFrame(
                    {
                        "agent_name": _safe_series(df, mapping.get("agent_name")),
                        "agent_code": _safe_series(df, mapping.get("agent_code"), default=""),
                        "hierarchy": _safe_series(df, mapping.get("hierarchy"), default=""),
                        "week": week,
                        "appointments": pd.to_numeric(_safe_series(df, column_name), errors="coerce"),
                        "month": _safe_series(df, mapping.get("month"), default=fallback_month),
                        "source_sheet": sheet_name,
                    }
                )
                wide_rows.append(temp)
            if not wide_rows:
                continue
            out = pd.concat(wide_rows, ignore_index=True)

        out["month"] = out["month"].apply(lambda value: normalize_month_value(value, fallback_month))
        out = out[out["agent_name"].astype(str).str.strip() != ""]
        out = out.dropna(subset=["week"])
        out["hierarchy"] = out["hierarchy"].apply(normalize_hierarchy)
        out["agent_key"] = out.apply(
            lambda row: build_agent_key(row["agent_name"], row["hierarchy"], alias_mapping=alias_mapping, agent_code=row["agent_code"]),
            axis=1,
        )
        parsed.append(out)

    if not parsed:
        empty = pd.DataFrame(columns=["month", "agent_name", "agent_code", "hierarchy", "week", "appointments", "source_sheet", "agent_key"])
        return empty, pd.DataFrame(columns=["dataset", "issue", "values"])

    combined = pd.concat(parsed, ignore_index=True)
    deduped, conflicts = _resolve_appointment_duplicates(combined)
    deduped["appointments"] = deduped["appointments"].fillna(0)
    return deduped.reset_index(drop=True), conflicts
