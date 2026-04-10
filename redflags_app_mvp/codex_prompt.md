# Prompt para continuar en Codex

Trabaja sobre este repositorio como un engineer senior. Objetivo: evolucionar el MVP `redflags_app_mvp` hacia una versión lista para producción sin romper la funcionalidad actual.

## Contexto de negocio

La app cruza archivos Excel de producción y citas por agente, calcula producción semanal a partir de MTD, detecta red flags y muestra dashboard/reportes.

## Estado actual

Ya existe un MVP en Streamlit con:
- carga manual de archivos Excel
- selección de hojas
- mapeo flexible de columnas
- soporte a layouts long y wide
- normalización de agentes
- cálculo semanal desde MTD
- reglas RF-001, RF-002 y RF-003
- dashboard, drill-down y exportes CSV/Excel/PDF
- persistencia local en `data/history`
- pruebas unitarias base

## Objetivo de esta iteración

Haz una evolución a `v1.1` con estos entregables:

1. **Calidad de datos**
   - agrega validaciones de columnas críticas por dataset
   - muestra errores específicos por hoja
   - detecta meses mezclados y evita procesarlos
   - agrega un resumen de calidad de datos en UI

2. **Matching de agentes**
   - mejora la normalización con alias configurables
   - soporta diccionario de equivalencias desde un CSV externo
   - agrega pruebas unitarias para matching

3. **Dashboard**
   - agrega filtros por mes, semana, jerarquía, severidad y tipo de bandera
   - agrega KPIs por jerarquía
   - mejora la tabla de sospechosos con ordenamiento por severidad/riesgo

4. **Riesgo**
   - crea un score de riesgo simple de 0 a 100 basado en reglas y umbrales
   - agrega columna `risk_score` a flags y summary
   - documenta la fórmula

5. **Persistencia**
   - reemplaza persistencia CSV por SQLite usando SQLAlchemy
   - conserva trazabilidad por archivo cargado, hoja y usuario

6. **DX**
   - agrega `Makefile`
   - agrega lint/format con `ruff`
   - agrega más tests
   - actualiza `README.md`

## Restricciones

- no rompas el flujo actual del MVP
- mantén Python + Streamlit
- conserva compatibilidad con layouts long y wide
- deja el código modular y legible
- ejecuta pruebas al final
- documenta decisiones relevantes en commits o en un changelog

## Criterios de aceptación

- la app sigue corriendo con `streamlit run app.py`
- tests verdes
- nuevo score visible en dashboard y reportes
- matching configurable por alias funcionando
- persistencia SQLite funcionando
