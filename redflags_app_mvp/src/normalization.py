from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


COMMON_SUFFIXES = {"JR", "SR", "LIC", "LIC.", "ING", "ING."}


def normalize_name(value: Any) -> str:
    text = normalize_text(value)
    parts = [part for part in text.split(" ") if part not in COMMON_SUFFIXES]
    return " ".join(parts)


def normalize_hierarchy(value: Any) -> str:
    return normalize_text(value)


def load_alias_mapping(csv_path: str | Path | None) -> dict[str, str]:
    if not csv_path:
        return {}
    path = Path(csv_path)
    if not path.exists():
        return {}

    aliases = pd.read_csv(path)
    if aliases.empty:
        return {}

    colset = {col.lower(): col for col in aliases.columns}
    alias_col = colset.get("alias")
    canonical_col = colset.get("canonical")
    if not alias_col or not canonical_col:
        return {}

    mapping: dict[str, str] = {}
    for _, row in aliases.iterrows():
        alias = normalize_name(row.get(alias_col))
        canonical = normalize_name(row.get(canonical_col))
        if alias and canonical:
            mapping[alias] = canonical
    return mapping


def resolve_alias(name: Any, alias_mapping: dict[str, str] | None = None) -> str:
    normalized = normalize_name(name)
    if not normalized:
        return ""
    if not alias_mapping:
        return normalized
    return alias_mapping.get(normalized, normalized)


def build_agent_key(
    agent_name: Any, hierarchy: Any = "", alias_mapping: dict[str, str] | None = None
) -> str:
    return (
        f"{resolve_alias(agent_name, alias_mapping)}::{normalize_hierarchy(hierarchy)}"
    )
