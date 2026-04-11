# Red Flags App MVP v2.0

Aplicación Streamlit para ingestión de Excel (producción MTD + citas), cálculo de métricas semanales/mensuales, detección de red flags, dashboard de monitoreo y generación de PDF ejecutivo.

## Cambios principales v2.0

- **Identidad canónica de agente**: matching por `agent_code` (si existe) o nombre normalizado; la jerarquía ya no participa en la llave de identidad.
- **Normalización robusta de nombres**: trim, lower, eliminación de acentos y puntuación, compresión de espacios.
- **Dedupe durable**:
  - Producción: dedupe por agente canónico + mes + semana + snapshot, sin sumar duplicados por hojas.
  - Citas: dedupe por agente canónico + mes + semana con política determinista (máximo) y conflicto auditado.
- **Derivación semanal desde MTD**: `weekly = mtd_actual - mtd_semana_previa`.
- **Conflictos de importación**: visibles en dashboard (no se suman ciegamente).
- **Overrides manuales**:
  - inclusión manual,
  - exclusión/exención manual por mes,
  - trazabilidad (razón, usuario, timestamp).
- **Conjunto final de monitoreo**:
  - `final = (auto_red_flags ∪ manual_include) - manual_exclude`.
- **PDF ejecutivo** usando el conjunto final (no solo auto red flags), con secciones de críticos, semanales, observación e inclusiones manuales.
- **Persistencia SQLite** de overrides y trazabilidad de corrida/importación.

## Flujo de importación

1. Subir Excel de Producción y Excel de Citas.
2. Seleccionar tipo de layout (`long` o `wide`).
3. Seleccionar hojas a procesar por archivo.
4. Mapear columnas detectadas (incluye soporte opcional de `agent_code` y `snapshot_date`).
5. Validar y procesar.
6. Revisar dashboard + conflictos + overrides manuales.
7. Revisar lista final previa a PDF y descargar reporte.

## Formato recomendado (v2)

### Producción
Columnas recomendadas:
- `snapshot_date` (YYYY-MM-DD)
- `report_month` (`YYYY-MM`)
- `agent_name`
- `agent_code` (recomendado)
- `hierarchy`
- `mtd_gross_production` / `production_mtd`

### Citas
Columnas recomendadas:
- `report_month` (`YYYY-MM`)
- `week_start_date`
- `week_end_date`
- `agent_name`
- `agent_code` (recomendado)
- `hierarchy`
- `appointment_count` / `appointments`

## Legacy support

Se mantienen cargas legacy con múltiples hojas como:
- producción: `produccion de gerentes abril`, `vip produccion`
- citas: `reporte de citas abril`

Siempre que se seleccionen y mapeen columnas en UI.

## Ejecutar localmente

```bash
make install
make run
```

## Pruebas

```bash
make test
```
