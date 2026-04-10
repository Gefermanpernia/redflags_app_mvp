# App de Monitoreo de Red Flags de Agentes

MVP funcional basado en el PRD para:

- subir Excel de producción y de citas
- seleccionar hojas a procesar
- mapear columnas desde la UI
- normalizar agentes
- convertir producción MTD a producción semanal cerrada
- detectar red flags semanales y mensuales
- revisar dashboard, detalle por agente e histórico
- exportar reportes en CSV, Excel y PDF

## Stack

- Python 3.11+
- Streamlit
- pandas + openpyxl
- reportlab

## Estructura

```text
redflags_app_mvp/
├── app.py
├── requirements.txt
├── README.md
├── src/
│   ├── config.py
│   ├── normalization.py
│   ├── parsers.py
│   ├── metrics.py
│   ├── red_flags.py
│   ├── reports.py
│   ├── persistence.py
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
└── data/
    └── history/
```

## Cómo correr

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Flujo de uso

1. En la pestaña **Carga**, sube el Excel de producción.
2. Selecciona si viene en formato **long** o **wide**.
3. Escoge las hojas a procesar.
4. Mapea columnas requeridas.
5. Repite el proceso con el Excel de citas.
6. Ajusta los umbrales en la barra lateral.
7. Haz clic en **Procesar archivos**.
8. Revisa **Dashboard**, **Detalle**, **Reportes** e **Histórico**.

## Supuestos de datos

### Producción
La app soporta dos layouts:

- **long**: una fila por agente/semana con `produccion_mtd`
- **wide**: una fila por agente con columnas `MTD semana 1..5`

### Citas
La app soporta dos layouts:

- **long**: una fila por agente/semana con `citas`
- **wide**: una fila por agente con columnas `citas semana 1..5`

## Reglas implementadas

### RF-001
Sin citas y alta producción mensual.

### RF-002
Pico en última semana sin actividad previa.

### RF-003
Pocas o cero citas con alta producción semanal.

## Trazabilidad

Cada corrida guarda artefactos en `data/history/<mes>/<timestamp>/` y registra auditoría en `data/audit_log.csv`.

## Pruebas

```bash
pytest
```

## Próximas mejoras sugeridas

- persistencia en base de datos
- autenticación y control por supervisor
- matching más inteligente de nombres
- bitácora de revisión por caso
- scoring de riesgo multicriterio
- carga automática diaria
