from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any


@dataclass
class ThresholdConfig:
    monthly_production_suspicious: float = 1500.0
    weekly_production_suspicious: float = 1500.0
    spike_last_week_threshold: float = 3000.0
    few_appointments_threshold: float = 1.0
    insignificant_production_threshold: float = 100.0
    severity_rule_a: str = "alta"
    severity_rule_b: str = "alta"
    severity_rule_c: str = "media-alta"
    use_open_week_partial: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


DEFAULT_THRESHOLDS = ThresholdConfig()

REQUIRED_PRODUCTION_LONG = {
    "agent_name": "Nombre del agente",
    "hierarchy": "Jerarquía (opcional)",
    "week": "Semana",
    "production_mtd": "Producción MTD",
    "month": "Mes (opcional si se define arriba)",
}

REQUIRED_PRODUCTION_WIDE = {
    "agent_name": "Nombre del agente",
    "hierarchy": "Jerarquía (opcional)",
    "month": "Mes (opcional si se define arriba)",
    "mtd_week_1": "MTD semana 1",
    "mtd_week_2": "MTD semana 2 (opcional)",
    "mtd_week_3": "MTD semana 3 (opcional)",
    "mtd_week_4": "MTD semana 4 (opcional)",
    "mtd_week_5": "MTD semana 5 (opcional)",
}

REQUIRED_APPOINTMENTS_LONG = {
    "agent_name": "Nombre del agente",
    "hierarchy": "Jerarquía (opcional)",
    "week": "Semana",
    "appointments": "Citas",
    "month": "Mes (opcional si se define arriba)",
}

REQUIRED_APPOINTMENTS_WIDE = {
    "agent_name": "Nombre del agente",
    "hierarchy": "Jerarquía (opcional)",
    "month": "Mes (opcional si se define arriba)",
    "appointments_week_1": "Citas semana 1",
    "appointments_week_2": "Citas semana 2 (opcional)",
    "appointments_week_3": "Citas semana 3 (opcional)",
    "appointments_week_4": "Citas semana 4 (opcional)",
    "appointments_week_5": "Citas semana 5 (opcional)",
}
