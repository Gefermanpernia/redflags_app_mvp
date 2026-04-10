from __future__ import annotations

import re
import unicodedata
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


COMMON_SUFFIXES = {
    "JR",
    "SR",
    "LIC",
    "LIC.",
    "ING",
    "ING.",
}


def normalize_name(value: Any) -> str:
    text = normalize_text(value)
    parts = [part for part in text.split(" ") if part not in COMMON_SUFFIXES]
    return " ".join(parts)


def normalize_hierarchy(value: Any) -> str:
    return normalize_text(value)


def build_agent_key(agent_name: Any, hierarchy: Any = "") -> str:
    return f"{normalize_name(agent_name)}::{normalize_hierarchy(hierarchy)}"
