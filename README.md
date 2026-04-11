# Red Flags App MVP v2.0

Aplicación Streamlit para ingestión de Excel (producción MTD + citas), cálculo semanal/mensual, detección de red flags y generación de reportes PDF ejecutivos.

## Arquitectura actual

- `app.py`: UI de carga, validación, dashboard, overrides manuales, y exportables.
- `src/parsers.py`: lectura de hojas Excel, mapeo de columnas y deduplicación determinística por agente canónico.
- `src/normalization.py`: normalización de identidad (`agent_code` preferido, fallback por nombre normalizado).
- `src/metrics.py`: derivación de producción semanal desde snapshots MTD y dataset consolidado semanal/mensual.
- `src/red_flags.py`: motor configurable de flags (mensual, semanal, pico final, observación).
- `src/pipeline.py`: unión de métricas + flags + lógica final de monitoreo con includes/excludes.
- `src/persistence.py`: auditoría de corridas, conflictos de importación y overrides manuales en SQLite.
- `src/reports.py`: exportación CSV/Excel y PDF ejecutivo basado en `final_monitoring_set`.

## Reglas de identidad y dedupe

1. Identidad canónica:
   - Si existe `agent_code`, se usa como key (`CODE::<code>`).
   - Si no existe, se usa nombre normalizado (`NAME::<normalized_name>`).
   - Jerarquía **no** participa en identidad.
2. Producción (mismo agente + mes + snapshot + semana):
   - valores idénticos => se conserva uno
   - si uno tiene neta y otro no => se conserva el más rico
   - conflicto de valores => no se suman; se conserva política determinística y se registra conflicto
3. Citas (mismo agente + mes + semana):
   - null vs número => gana número
   - iguales => se conserva uno
   - conflicto no-nulo => política `max_appointments` + warning de conflicto

## Flujo de importación

1. Subir archivo
2. Elegir formato (long/wide) y hojas
3. Mapear columnas
4. Validar
5. Procesar + dedupe + auditoría + conflictos

Soporte legacy preseleccionado:
- Producción: `produccion de gerentes abril`, `vip produccion`
- Citas: `reporte de citas abril`

## Monitoreo final y overrides

La regla implementada:

`final_monitoring_set = (auto_red_flagged_agents UNION manually_included_agents) MINUS manually_excluded_agents`

- Includes/excludes se guardan con razón, timestamp, usuario y mes.
- El PDF se genera desde la selección final editable en UI.

## Ejecución local

```bash
make install
make run
```

## Calidad

```bash
make lint
make test
```
