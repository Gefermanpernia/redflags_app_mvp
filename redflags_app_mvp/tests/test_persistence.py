from __future__ import annotations

import pandas as pd

from src.persistence import _week_of_month_sunday_closure


def test_week_of_month_sunday_closure() -> None:
    assert _week_of_month_sunday_closure(pd.Timestamp("2026-04-01")) == 1
    assert _week_of_month_sunday_closure(pd.Timestamp("2026-04-05")) == 1
    assert _week_of_month_sunday_closure(pd.Timestamp("2026-04-06")) == 2
    assert _week_of_month_sunday_closure(pd.Timestamp("2026-04-12")) == 2
