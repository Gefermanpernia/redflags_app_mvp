# Red Flags App MVP v1.1

Evolución del MVP a una versión operable para producción ligera, manteniendo flujo Streamlit.

## Novedades v1.1

- Validaciones de calidad por hoja y dataset.
- Bloqueo de procesamiento cuando hay meses mezclados en una misma hoja.
- Resumen de calidad en UI.
- Matching de agentes con alias configurables vía CSV (`alias,canonical`).
- Score de riesgo `risk_score` (0-100) agregado a flags y summary.
- Dashboard con filtros por mes/semana/jerarquía/severidad/tipo de bandera.
- KPIs por jerarquía y priorización de sospechosos por riesgo.
- Persistencia migrada a SQLite + SQLAlchemy con trazabilidad de archivo/hoja/usuario.
- DX: `Makefile`, `ruff`, tests ampliados.

## Fórmula de riesgo

`risk_score = puntos_regla + puntos_severidad + intensidad_métrica`, limitado entre 0 y 100.

- `puntos_regla`: RF-001=25, RF-002=30, RF-003=20.
- `puntos_severidad`: baja=5, media=12, media-alta=18, alta=25, crítica=35.
- `intensidad_métrica`: escala por cuánto excede cada umbral por regla.

## Uso rápido

```bash
make install
make run
```

## Calidad y pruebas

```bash
make lint
make test
```
