# Arquitectura

## 1. Vista general

```
                        data/raw/*.parquet          ← INMUTABLE (solo lectura)
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │ BRONZE - qué llegó y qué se intentó procesar                          │
  │   · SHA-256 del contenido, tamaño, esquema, nº de filas               │
  │   · Validación de archivo y esquema  ─────────► error crítico → exit 1│
  │   · Resolución de la fecha del corte                                  │
  │   · Decisión de idempotencia  ────────────────► ya procesado → SKIP   │
  │   → file_registry · pipeline_runs · run_alerts                        │
  └───────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │ SILVER - qué es utilizable                                            │
  │   Una sola pasada de SQL sobre el parquet:                            │
  │   · Tipado: fecha multi-formato, DOUBLE → DECIMAL(20,2)               │
  │   · Normalización: type → IN/OUT, fund y product contra catálogo      │
  │   · Validación por registro según el contrato                         │
  │   · Deduplicación exacta + ordinal de ocurrencia                      │
  │   · movement_key y row_hash (serialización estable)                   │
  │   → stg_movements · rejected_records · data_quality_flags             │
  └───────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │ DETECCIÓN DE CAMBIOS  (aún no se escribe nada en GOLD)                │
  │   FULL OUTER JOIN  stg_movements ⟗ movements_current  ON movement_key │
  │   → NEW · UPDATED · DELETED · UNCHANGED · REACTIVATED                 │
  │   → columnas modificadas, con comparación NULL-safe                   │
  └───────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │ GUARDAS DE SEGURIDAD                                                  │
  │   % de bajas · caída de volumen · variación monetaria · mínimo        │
  │   umbral de fallo superado ───────────► exit 3, NADA se aplica        │
  └───────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │ GOLD - cuál es la verdad hoy y cómo llegamos                          │
  │   ╔═══════════ BEGIN TRANSACTION ═══════════════════════════════════╗ │
  │   ║ 1. cerrar versiones vigentes afectadas                          ║ │
  │   ║ 2. abrir versiones nuevas                                       ║ │
  │   ║ 3. actualizar el estado vigente (altas, correcciones, bajas)    ║ │
  │   ║ 4. escribir la bitácora de cambios                              ║ │
  │   ║ 5. invariantes estructurales   ──── violación ──► ROLLBACK      ║ │
  │   ║ 6. reconciliación (12 controles) ── falla ──────► ROLLBACK      ║ │
  │   ║ 7. marcar la ejecución como SUCCESS                             ║ │
  │   ╚═══════════ COMMIT ══════════════════════════════════════════════╝ │
  │   → movements_current · movements_history · movement_changes          │
  │     movement_change_fields · reconciliation_results                   │
  └───────────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
      ┌──────────────────┐            ┌────────────────────┐
      │ Vistas analíticas│            │ Reportes CSV / MD  │
      │ + detección de   │            │ + insights.md      │
      │   anomalías      │            │ + reconciliación   │
      │   → anomalies    │            │                    │
      └────────┬─────────┘            └────────────────────┘
               ▼
      ┌──────────────────┐
      │ Tablero Streamlit│  (solo lectura)
      └──────────────────┘
```

## 2. Por qué tres capas y no una

Justificación completa en [`decisions.md`](decisions.md) ADR-003. Cada capa
responde a una pregunta que el enunciado plantea explícitamente:

| Capa | Pregunta que responde | Sin ella se pierde |
| --- | --- | --- |
| Bronze | ¿Qué archivo llegó y era procesable? | La idempotencia por hash y la auditoría del origen |
| Silver | ¿Qué era utilizable y qué no, y por qué? | La cuarentena auditable; los rechazos serían un contador |
| Gold | ¿Cuál es la verdad hoy y cómo llegamos aquí? | El histórico y la trazabilidad de cambios |

No se implementaron como tres almacenes físicos separados: Silver es una zona de
trabajo que se vacía por `run_id` al inicio de cada ejecución. Triplicar el
almacenamiento no habría aportado nada a este volumen.

## 3. Responsabilidad de cada módulo

| Módulo | Responsabilidad |
| --- | --- |
| `src/config.py` | Carga de `pipeline.yml` + `data_contract.yml`, resolución de rutas, sobreescritura por variables de entorno |
| `src/logging_config.py` | Logging estructurado con `run_id` en cada línea; formato texto o JSON |
| `src/database.py` | Conexión DuckDB, DDL idempotente, `transaction()`, ajuste de memoria y desbordamiento a disco |
| `src/ingestion.py` | Descubrimiento, SHA-256, metadatos, validación de archivo y esquema, fecha del corte |
| `src/normalization.py` | Genera las expresiones SQL desde el contrato y ejecuta la capa de staging |
| `src/change_detection.py` | Clasificación en NEW/UPDATED/DELETED/UNCHANGED/REACTIVATED |
| `src/persistence.py` | Escritura de GOLD e invariantes estructurales |
| `src/observability.py` | Métricas, alertas y guardas de seguridad |
| `src/reconciliation.py` | Controles de cuadre y sus reportes por ejecución |
| `src/analytics.py` | Vistas analíticas y detección de anomalías |
| `src/reporting.py` | Reportes CSV e `insights.md` |
| `src/pipeline.py` | Orquestación, idempotencia, CLI, códigos de salida |
| `src/presentation.py` | Transformaciones de pandas para el tablero (fechas legibles, escala de montos), sin depender de Streamlit |

**Lo que deliberadamente NO se hizo:** un módulo `metadata.py` con dos funciones
triviales, un `schema_validation.py` separado de `ingestion.py` (la validación de
esquema solo tiene sentido junto a la lectura del archivo), ni clases donde
bastan funciones puras. `observability.py` incluye las guardas porque comparten
el mecanismo de alerta: separarlas habría creado una dependencia circular o un
módulo de tres líneas. `presentation.py` sí se separó del tablero: vive en
`src/` para poder probarse sin instalar Streamlit.

## 4. Dónde vive la lógica

El trabajo por fila está **en SQL**, no en Python. Python orquesta, decide y
reporta; DuckDB calcula.

| Archivo SQL | Qué resuelve |
| --- | --- |
| `sql/schema.sql` | DDL completo, idempotente, parametrizado por el tipo decimal |
| `sql/staging.sql` | Tipado, normalización, validación, deduplicación, claves y hashes - en una sola pasada |
| `sql/change_detection.sql` | El `FULL OUTER JOIN` y la clasificación |
| `sql/persistence.sql` | Las 8 sentencias del bloque transaccional |
| `sql/reconciliation.sql` | Los 12 controles de cuadre |
| `sql/analytics.sql` | 16 vistas analíticas |

Consecuencia práctica: no hay ni un bucle de Python sobre filas de datos en todo
el pipeline.

## 5. Flujo de datos con los archivos entregados

```
movimientos_dia_T.parquet     50.000 filas
  ├─ 1.543 → cuarentena (amount nulo)
  └─ 48.457 válidas ─────────────► 48.457 NEW
                                   estado vigente: 48.457 · 1.133.406.868.796,56

movimientos_dia_T1.parquet    49.000 filas
  ├─ 1.440 → cuarentena (amount nulo)
  └─ 47.560 válidas ─┬─ 34.075 UNCHANGED
                     ├─  3.724 UPDATED
                     └─  9.761 NEW
                        + 10.658 DELETED (baja lógica)
                                   estado vigente: 47.560 · 1.113.305.841.982,36
                                   histórico:      61.942 versiones
```

Comprobación: `48.457 + 9.761 − 10.658 = 47.560` ✓

## 6. Entradas y salidas

Generadas automáticamente por `docker compose up` (o `python -m src.pipeline`):

| Dirección | Ruta | Contenido |
| --- | --- | --- |
| Entrada | `data/raw/*.parquet` | Cortes diarios. Solo lectura, nunca se modifican. |
| Entrada | `config/*.yml` | Contrato de datos y configuración operativa. |
| Salida | `data/database/movements.duckdb` | Base consultable con todas las tablas y vistas. |
| Salida | `data/reports/*.csv` | 12 reportes tabulares (`src/reporting.py`), regenerados en cada ejecución. |
| Salida | `data/reports/insights.md` | Documento de insights con método y limitaciones. |
| Salida | `data/reports/reconciliation_<run_id>.csv` + `_aggregates.csv` + `.md` | Cuadre por ejecución, 3 archivos (`src/reconciliation.py`). Con dos cortes son 6, más el consolidado `reconciliation_all_runs.csv`. |
| Salida | `data/reports/pipeline.log` | Log completo con `run_id`. |

Generado aparte, con un comando explícito - **no** sale de `docker compose up`:

| Dirección | Ruta | Contenido |
| --- | --- | --- |
| Salida | `data/reports/data_profile.md` + `data_profile_summary.csv` | Perfilado de los archivos de entrada. Requiere `docker compose run --rm profile` (`scripts/profile_data.py`). |

## 7. Tratamiento de errores

| Nivel | Qué lo produce | Efecto | Código de salida |
| --- | --- | --- | ---: |
| **CRÍTICO** | Archivo ausente, ilegible o vacío; columna obligatoria ausente; tipo incompatible; fecha de corte irresoluble; corte fuera de orden; tasa de rechazo por encima del umbral | El pipeline se detiene. No se aplica nada. | 1 |
| **GUARDA** | Umbral de bajas, de caída de volumen o de variación monetaria superado | La carga se aborta **sin aplicar bajas**. Ejecución `FAILED_GUARD`. | 3 |
| **ESTRUCTURAL** | Invariante violada o reconciliación descuadrada | `ROLLBACK` completo. Ejecución `FAILED`. | 2 |
| **DE REGISTRO** | `id` nulo o vacío, fecha inválida, `type` desconocido, importe nulo o no finito | La fila va a cuarentena. El pipeline continúa. | 0 |
| **AVISO** | Fondo o producto fuera de catálogo, formato de id atípico, clave ambigua, pérdida de precisión | Se marca en `data_quality_flags`. La fila se conserva. | 0 |
| **INFO** | Importe negativo o cero, descripción o comercio nulos | Se marca y se reporta. | 0 |

Cuando se procesan varios archivos y uno falla, **el procesamiento se detiene**:
aplicar T+2 sobre un estado que no se pudo construir correctamente en T+1
produciría bajas falsas.

## 8. Riesgos de la arquitectura

| Riesgo | Mitigación |
| --- | --- |
| La identidad derivada no coincide con la del origen | Configurable en un solo punto; supuesto medido y documentado; filas dudosas marcadas |
| Un corte truncado destruye el estado | Guardas de dos niveles; sin aplicar nada al fallar |
| Escritura parcial por fallo dentro del bloque GOLD | Todo GOLD en una transacción; las pruebas de rollback comparan el estado completo |
| Fallo entre el `COMMIT` y el fin de la carga | `pipeline_runs=SUCCESS` y `file_registry=PROCESSED` se confirman en la misma transacción; un fallo de reporte genera `REPORTING_FAILED` sin falsear el estado de la carga |
| Un único escritor (limitación de DuckDB) | Suficiente para un batch diario; ruta de migración documentada |
| El histórico crece sin límite | La base completa llegó a 14,38 GB tras dos cortes sintéticos de 10M y 9,8M filas; ver estrategias de evolución más abajo |

## 9. Estrategias de evolución

1. **Más volumen del que cabe cómodamente**: particionar `movements_history` por
   `snapshot_date` en Parquet y dejar en DuckDB solo el estado vigente.
2. **Concurrencia de escritura o muchos consumidores**: migrar a PostgreSQL. El
   SQL es prácticamente portable; los cambios son `sha256()`, `list_filter` y las
   funciones de fecha.
3. **Escala de lakehouse**: Delta Lake o Iceberg con `MERGE INTO`, conservando la
   misma clave de negocio y la misma lógica de clasificación.
4. **Orquestación**: el pipeline ya es idempotente y devuelve códigos de salida
   correctos, así que se integra en Airflow o Dagster sin cambios.
