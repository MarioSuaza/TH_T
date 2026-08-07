# Pipeline de Movimientos Financieros — Tyba

Pipeline de ingeniería de datos que ingiere cortes diarios de movimientos
financieros en Parquet y mantiene una base consultable, historificada y
auditable a lo largo del tiempo.

```bash
docker compose up --build
```

Eso es todo. Sin configuración adicional.

---

## Índice

1. [Qué resuelve](#1-qué-resuelve)
2. [Arquitectura](#2-arquitectura)
3. [Requisitos](#3-requisitos)
4. [Ejecución](#4-ejecución)
5. [Datos de entrada](#5-datos-de-entrada)
6. [Procesamiento](#6-procesamiento)
7. [Reglas de cambios](#7-reglas-de-cambios)
8. [Calidad de datos](#8-calidad-de-datos)
9. [Idempotencia](#9-idempotencia)
10. [Modelo de datos](#10-modelo-de-datos)
11. [Insights y tablero](#11-insights-y-tablero)
12. [Pruebas](#12-pruebas)
13. [Escalabilidad](#13-escalabilidad)
14. [Decisiones técnicas](#14-decisiones-técnicas)
15. [Supuestos y limitaciones](#15-supuestos-y-limitaciones)
16. [Solución de problemas](#16-solución-de-problemas)

---

## 1. Qué resuelve

Cada día llega un archivo con el **estado completo** de los movimientos
financieros. De un corte al siguiente aparecen registros nuevos, otros se
corrigen, algunos desaparecen, y los datos no llegan limpios. El pipeline
consolida esa evolución sin perder trazabilidad ni duplicar información.

**Sobre la identidad del movimiento.** El glosario del enunciado documenta una
columna `id` ("identificador de la transacción") que no existe en los archivos
entregados: la columna real es `id_cliente`, con 3.000 valores distintos sobre
50.000 filas. Es un identificador de cliente, no de transacción. El pipeline
construye una **clave de negocio derivada** (`id_cliente` + fecha + producto +
tipo + fondo, más un ordinal de desempate) a partir de los atributos que no
cambian cuando un movimiento se corrige. El detalle y la evidencia están en
[`docs/analysis_and_assumptions.md`](docs/analysis_and_assumptions.md), A-01.

**Resultado sobre los archivos entregados:**

| Corte | Leídas | Válidas | Cuarentena | NEW | UPDATED | DELETED | UNCHANGED | Vigentes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| T (2024-10-15) | 50.000 | 48.457 | 1.543 | 48.457 | 0 | 0 | 0 | 48.457 |
| T+1 (2024-10-16) | 49.000 | 47.560 | 1.440 | 9.761 | 3.724 | 10.658 | 34.075 | 47.560 |

12 controles de reconciliación por corte, todos cuadran.

**Incluye:** ingesta, validación contra contrato, normalización, cuarentena,
detección de cambios, histórico SCD2, reconciliación financiera, analítica,
detección de anomalías, tablero y suite de pruebas.
**No incluye:** orquestación externa ni despliegue — el pipeline es idempotente
y devuelve códigos de salida estándar, así que se integra en cualquier
orquestador sin cambios.

---

## 2. Arquitectura

```
data/raw/*.parquet  →  BRONZE  →  SILVER  →  cambios  →  guardas  →  GOLD  →  reportes
   (inmutable)         hash,      tipado,     FULL       umbrales    trans-    + tablero
                       esquema,   normali-    OUTER      de          acción
                       registro   zación,     JOIN       seguridad   atómica
                                  cuarentena
```

Diagrama completo, responsabilidades por módulo y tratamiento de errores en
[`docs/architecture.md`](docs/architecture.md).

**Capas.** *Bronze*: qué llegó (archivos intactos + `file_registry` con hash).
*Silver*: qué es utilizable (tipado, normalizado, validado, con cuarentena).
*Gold*: cuál es la verdad hoy y cómo se llegó ahí (estado vigente + histórico +
bitácora). Son capas lógicas, no tres almacenes físicos: Silver es una zona de
trabajo que se vacía en cada ejecución. Justificación en
[`docs/decisions.md`](docs/decisions.md) ADR-003.

**Persistencia:** DuckDB, archivo único en `data/database/movements.duckdb`.
12 tablas, 16 vistas.

**Estructura del repositorio**

```
solution/
├── config/          data_contract.yml · pipeline.yml
├── data/
│   ├── raw/         los .parquet entregados (copiados aquí, intactos)
│   ├── database/    movements.duckdb (generado)
│   └── reports/     CSV, insights.md, reconciliaciones, log (generados)
├── docs/            análisis, arquitectura, modelo de datos y decisiones técnicas
├── sql/             schema · staging · change_detection · persistence
│                    reconciliation · analytics
├── src/             13 módulos
├── scripts/         profile_data.py · benchmark.py
├── dashboard/       app.py (Streamlit)
├── tests/           183 pruebas en Docker (178 sin Streamlit)
├── Dockerfile · docker-compose.yml · requirements.txt · Makefile
└── README.md
```

---

## 3. Requisitos

| Requisito | Versión |
| --- | --- |
| Docker | 20.10 o superior |
| Docker Compose | v2 (`docker compose`, sin guion) |
| RAM | 2 GB para la ejecución normal; 12 GB asignados a Docker para cortes de 10M filas |
| Disco | ~500 MB de imagen + el tamaño de la base |
| Puerto | 8501, solo para el tablero |

Sin Docker: Python 3.11 y `pip install -r requirements.txt`.

Sin servicios externos, credenciales ni variables obligatorias. El pipeline
corre **sin acceso a red** (`network_mode: none`).

---

## 4. Ejecución

### Lo mínimo

```bash
docker compose up --build
```

Construye la imagen, inicializa la base, procesa los cortes pendientes en
orden, valida, reconcilia, genera los reportes y termina con código 0.

### Con el tablero

```bash
docker compose --profile dashboard up --build
# → http://localhost:8501
```

El tablero solo arranca cuando el pipeline termina bien, y va en un perfil
aparte para que `docker compose up` termine en vez de quedarse sirviendo una
web.

### Otros comandos

```bash
docker compose run --rm tests            # 183 pruebas
docker compose run --rm profile          # perfila los parquet de data/raw
docker compose run --rm shell            # consola Python con la base en solo lectura
docker compose run --rm benchmark        # prueba de escala con datos sintéticos
docker compose down                      # parar
make help                                # todos los atajos
```

### Reprocesar y consultar

```bash
make reset && docker compose up --build     # desde cero; data/raw queda intacto
docker compose run --rm pipeline src.pipeline \
    --input-directory /app/data/raw --force # reprocesa sin borrar nada

docker compose run --rm shell               # consola con la base en solo lectura
>>> con.sql("SELECT * FROM v_summary_by_fund")
```

### Reportes

Todo queda en `data/reports/`:

| Archivo | Contenido |
| --- | --- |
| `insights.md` | **Empezar por aquí.** Insights con método, periodo y limitaciones |
| `data_profile.md` | Perfilado de los archivos de entrada |
| `reconciliation_<run_id>.md` / `.csv` | Cuadre de una ejecución |
| `reconciliation_aggregates_<run_id>.csv` | Sumas por dimensión de esa ejecución |
| `change_summary.csv` | Cambios por corte y tipo |
| `financial_summary.csv` | Sumas por fondo, producto, comercio y tipo |
| `daily_metrics.csv` | Métricas por fecha del movimiento |
| `data_quality_metrics.csv` | Tasas de validez y rechazo por ejecución |
| `rejections_by_code.csv` | Motivos de rechazo |
| `quality_flags.csv` | Avisos no bloqueantes agrupados por código |
| `field_change_frequency.csv` | Frecuencia de corrección por columna |
| `snapshot_summary.csv` | Una fila por corte procesado con éxito |
| `anomalies.csv` | Eventos que merecen revisión |
| `top_movements.csv` | Movimientos vigentes de mayor magnitud |
| `top_changes_by_impact.csv` | Correcciones y bajas de mayor impacto |
| `reconciliation_all_runs.csv` | Historial de los 12 controles de todas las ejecuciones |
| `pipeline.log` | Log completo con `run_id` |

De estos, `data_quality_metrics.csv` y `reconciliation_all_runs.csv` son series
históricas por ejecución para análisis externo: no alimentan `insights.md` ni
el tablero. El resto sí.

### Sin Docker

```bash
pip install -r requirements.txt
python -m src.pipeline --input-directory data/raw
python -m pytest
streamlit run dashboard/app.py
```

---

## 5. Datos de entrada

### Ubicación y formato

`data/raw/*.parquet`. Los archivos de Tyba están copiados ahí y **nunca se
modifican**: el pipeline solo los lee.

| Archivo | Filas | SHA-256 |
| --- | ---: | --- |
| `movimientos_dia_T.parquet` | 50.000 | `9eb7c1156472e93a…` |
| `movimientos_dia_T1.parquet` | 49.000 | `ee067cc5bee044f1…` |

### Esquema esperado

| Columna | Tipo | Obligatoria | Nulos permitidos |
| --- | --- | :---: | :---: |
| `id_cliente` | string | Sí | No |
| `date` | string | Sí | No |
| `product` | string | Sí | No |
| `type` | string | Sí | No |
| `fund` | string | Sí | No |
| `amount` | double | Sí | No (configurable) |
| `description` | string | Sí | Sí |
| `commercial_name` | string | Sí | Sí |

Contrato completo con dominios, severidades y ejemplos en
[`config/data_contract.yml`](config/data_contract.yml).

El **orden de las columnas es irrelevante** (se accede por nombre); las
columnas adicionales se ignoran y quedan registradas.

### Fechas

`date` llega en **dos formatos en el mismo archivo**: `YYYY-MM-DD` (46.518
filas en T) y `DD/MM/YYYY` (3.482 filas). Ambos se parsean a la misma fecha y,
lo que importa, a la **misma clave de movimiento**.

La **fecha del corte** se resuelve en este orden de prioridad:

1. `--snapshot-date` en la CLI
2. Fecha ISO en el nombre del archivo: `movimientos_2026-08-05.parquet`
3. Secuencia declarada en `config/pipeline.yml` (usada para los archivos
   entregados, cuyos nombres no llevan fecha)
4. `max(date)` del contenido, con aviso

### Añadir un corte nuevo

```bash
cp movimientos_2026-08-05.parquet data/raw/
docker compose up --build
```

El pipeline ordena lo pendiente por fecha de corte, procesa solo lo nuevo y
omite lo ya aplicado. No hay que tocar código ni configuración. Si el archivo
no lleva fecha en el nombre, se pasa con `--snapshot-date`.

---

## 6. Procesamiento

| Fase | Qué hace |
| --- | --- |
| **Ingesta** | Descubre los archivos, calcula SHA-256, lee metadatos sin materializar datos, valida archivo y esquema, resuelve la fecha del corte, decide idempotencia |
| **Normalización** | Parseo multi-formato de fechas, `type` → `IN`/`OUT`, `fund` y `product` contra catálogo, `DOUBLE` → `DECIMAL(20,2)`. El valor original se conserva en columnas `*_original` |
| **Validación** | Reglas del contrato por registro; primer error determinista |
| **Cuarentena** | Los inválidos van a `rejected_records` con su fila original completa en JSON |
| **Duplicados** | Exactos: se colapsan y se cuentan. Clave repetida: ordinal determinista + marca |
| **Detección de cambios** | `FULL OUTER JOIN` contra el estado vigente por `movement_key`; comparación por `row_hash`. Corre antes de abrir la transacción de escritura |
| **Guardas** | Umbrales de bajas, volumen y variación monetaria. Si se superan, no se aplica nada |
| **Persistencia** | Bloque transaccional: cerrar versiones, abrir nuevas, actualizar vigente, escribir bitácora |
| **Reconciliación** | 12 controles dentro de la misma transacción; si alguno falla, `ROLLBACK` |
| **Analítica** | 16 vistas, 5 detectores de anomalías, 12 CSV e `insights.md` |

### La clave del movimiento

```
movement_key = SHA-256( "v1" + SEP + id_cliente + SEP + fecha ISO + SEP + product + SEP + type + SEP + fund + SEP + ordinal )
row_hash     = SHA-256( los 5 anteriores + amount + description + commercial_name )
```

`SEP` es el carácter de control U+001F (separador de unidad, invisible,
ausente en los datos). Token de nulo U+001E+`NULL`, fecha ISO y decimal de
escala fija. Ni el orden de las columnas, ni el orden de las filas, ni el
formato de origen alteran la identidad — cada caso tiene su prueba.

---

## 7. Reglas de cambios

| Tipo | Condición | Efecto |
| --- | --- | --- |
| `NEW` | La clave no existía | Alta en vigente + versión 1 en histórico |
| `UPDATED` | La clave existía y el `row_hash` cambió | Se cierra la versión anterior, se abre la siguiente, se registran las columnas modificadas |
| `DELETED` | La clave estaba vigente y no llega en el corte | Baja lógica: `is_active = false`, `deleted_at`, versión cerrada con `is_deleted = true` |
| `UNCHANGED` | La clave existía y el `row_hash` es idéntico | Solo se refresca `last_seen_at`. No crea versión ni evento |
| `REACTIVATED` | Una clave dada de baja reaparece | Se reactiva y se abre una versión nueva |

### Columnas modificadas

Cada corrección registra qué cambió, con el valor anterior y el nuevo, en
`movement_change_fields` — estructura relacional, no JSON, agregable con SQL
plano:

```
movement_key: 75fd41f8e65b…
change_type:  UPDATED
changed_columns: [amount, description]
  amount:      250.00 → 275.00
  description: "Retiro parcial" → "Retiro total"
amount_delta: +25.00
```

### Nulos en la comparación

`IS DISTINCT FROM` en toda la comparación: dos nulos son iguales, nulo contra
valor es un cambio. El `row_hash` usa un token explícito de nulo, así que la
propiedad se cumple también a nivel de hash.

---

## 8. Calidad de datos

### Inconsistencias encontradas

| Hallazgo | T | T+1 | Decisión |
| --- | ---: | ---: | --- |
| No existe columna `id`; `id_cliente` tiene 3.000 valores en 50.000 filas | – | – | Clave de negocio derivada |
| `date` en dos formatos distintos | 3.482 DMY | 3.433 DMY | Parseo multi-formato |
| `type` con 10 variantes textuales | 10 | 10 | Canonicalización a `IN`/`OUT` |
| `fund` con 23 variantes de 7 valores reales | 23 | 23 | Catálogo case-insensitive + colapso de espacios |
| `amount` nulo | 1.543 | 1.440 | Cuarentena |
| `amount` negativo, solo en `type = IN` | 1.034 | 989 | Se reporta, no se corrige |
| `amount` cero | 940 | 903 | Se marca, se conserva |
| Misma clave con datos distintos (tras cuarentena) | 220 filas | 202 | Ordinal determinista + marca |
| `description` nulo | 4.563 | 4.485 | Permitido |
| `commercial_name` nulo | 8.307 | 8.167 | Permitido |
| Duplicados exactos | 0 | 0 | Lógica implementada y probada igualmente |

### Severidades

| Severidad | Efecto | Código de salida |
| --- | --- | ---: |
| `CRITICAL` | El pipeline se detiene, no se aplica nada | 1 (o 3 si es guarda) |
| `RECORD_ERROR` | La fila va a cuarentena, el pipeline continúa | 0 |
| `WARNING` | La fila se conserva, marcada | 0 |
| `INFO` | Observación | 0 |

32 códigos de error declarados en `config/data_contract.yml`.

### Umbrales

| Control | Aviso | Fallo | Observado |
| --- | ---: | ---: | ---: |
| Tasa de rechazo | 2% | 15% | 3,09% → avisa |
| Porcentaje de bajas | 10% | 30% | 21,99% → avisa |
| Caída de volumen | 10% | 30% | 1,85% |
| Variación monetaria | 20% | 50% | 1,77% |

Calibrados para que la alerta se dispare sobre los datos reales sin bloquear la
carga.

### Reconciliaciones

12 controles por ejecución, dentro de la transacción. Si alguno falla,
`ROLLBACK`.

```
rows_read = rows_valid + rows_exact_dupes + rows_rejected
NEW + UPDATED + DELETED + UNCHANGED + REACTIVATED + ya_dados_de_baja = claves del FULL OUTER JOIN
NEW + UPDATED + UNCHANGED + REACTIVATED = claves distintas del corte
vigentes_después = vigentes_antes + NEW + REACTIVATED − DELETED
monto_después = monto_antes + Σ(impacto de los cambios)
máximo una fila vigente por movimiento
versiones consecutivas sin huecos: max(version) − min(version) + 1 = count(*)
```

`rows_valid` cuenta filas normalizadas distintas; `rows_exact_dupes` repone la
multiplicidad de duplicados colapsados. El control de "sin huecos" se compara
contra `max − min + 1` en vez de contra `max` porque, con retención de
histórico activa (`persistence.history_retention_days`), la secuencia
conservada puede empezar en una versión mayor que 1 sin que eso sea una
inconsistencia. Ver
`test_reconciliation_survives_purge_that_shifts_min_version` en
[`tests/test_history_retention.py`](tests/test_history_retention.py).

---

## 9. Idempotencia

### Cómo funciona

La identidad de un corte es **(SHA-256 del contenido, fecha del corte)**, no
el nombre ni la ruta del archivo.

| Situación | Qué pasa |
| --- | --- |
| El mismo archivo, el mismo corte | Se omite y se registra el motivo |
| El mismo contenido, otra ruta u otro nombre | Se omite: la identidad es el contenido |
| El mismo contenido, corte posterior | Se procesa: el origen revirtió a un estado previo, y eso es un cambio real |
| El mismo nombre, contenido distinto | Se procesa como corte nuevo (configurable) |
| Un corte anterior al último aplicado | Se rechaza (configurable) |
| `--force` | Reprocesa; el estado consultable final es idéntico, pero avanza el contador de versión SCD |

Ese tercer caso es el que hay que explicar: usar solo el hash haría que el
pipeline ignorara silenciosamente una reversión legítima del origen.

### Verificado

Ejecutando dos veces sobre los archivos reales: cero filas alteradas, cero
versiones nuevas, cero ejecuciones duplicadas, métricas idénticas.

```
20241015__9eb7c1156472  SUCCESS  NEW=48457
20241016__ee067cc5bee0  SUCCESS  NEW=9761 UPDATED=3724 DELETED=10658 UNCHANGED=34075
# segunda ejecución:
movimientos_dia_T.parquet    SKIPPED
movimientos_dia_T1.parquet   SKIPPED
```

---

## 10. Modelo de datos

| Tabla | Contenido |
| --- | --- |
| `movements_current` | Estado vigente. PK sobre `movement_key`: el motor garantiza "una fila por movimiento" |
| `movements_history` | SCD2: una fila por versión, con `valid_from`, `valid_to`, `is_current`, `is_deleted` |
| `movement_changes` | Bitácora de eventos con impacto monetario |
| `movement_change_fields` | Detalle columna a columna, relacional |
| `rejected_records` | Cuarentena con la fila original en JSON |
| `data_quality_flags` | Avisos no bloqueantes |
| `file_registry` | Trazabilidad de archivos; base de la idempotencia |
| `pipeline_runs` | Auditoría por ejecución, incluida la versión y el SHA-256 del contrato aplicado |
| `run_alerts` | Alertas emitidas por las guardas de seguridad |
| `reconciliation_results` | Los controles de cuadre |
| `anomalies` | Eventos que merecen revisión |
| `stg_movements` | Zona de trabajo, se vacía en cada ejecución |

**Vistas.** 16 vistas definidas en [`sql/analytics.sql`](sql/analytics.sql),
creadas al final de cada ejecución — por ejemplo `v_movements_active`
(vigentes), `v_summary_by_fund` (cortes financieros por fondo) o
`v_top_changes_by_impact` (correcciones y bajas de mayor impacto). Listado
completo, diccionario, relaciones y consultas de ejemplo en
[`docs/data_model.md`](docs/data_model.md).

---

## 11. Insights y tablero

### Documento de insights

`data/reports/insights.md`, regenerado en cada ejecución. Cada métrica indica
su método de cálculo, periodo, filtros y limitación.

Algunos resultados sobre los datos entregados:

- 47.560 movimientos vigentes de 3.000 clientes, entre 2024-09-15 y
  2024-10-16.
- Monto vigente 1.113.305.841.982,36. Balance neto (IN − OUT) 76.810.657.788,24.
- La distribución por fondo, producto y comercio es prácticamente plana: no
  hay concentración relevante en ninguna dimensión, coherente con datos
  generados sintéticamente.
- `amount` es la columna que más se corrige, seguida de `description`.
- El día 2024-10-16 tiene 304 movimientos frente a una media de 1.486
  (z = −5,25). Con solo dos cortes no se puede afirmar la causa.

### Eventos que merecen revisión

Detección explicable, sin machine learning. Ninguno afirma que exista fraude o
error.

| Código | Método |
| --- | --- |
| `AMOUNT_OUTLIER` | Regla intercuartílica de Tukey (×3) por (fondo, tipo) |
| `DAILY_VOLUME_OUTLIER` | Z-score del volumen diario |
| `NEW_CATEGORY_VALUE` | Valores fuera del catálogo observado |
| `HIGH_DELETION_SHARE` | Porcentaje de bajas sobre los vigentes antes del corte |
| `NEGATIVE_AMOUNT_CONCENTRATION` | Concentración del signo negativo por `type` |

El hallazgo más llamativo: el 100% de los importes negativos está en
`type = IN` (1.034 de 1.034 en T, 989 de 989 en T+1). El enunciado no define el
signo, así que se reporta como pregunta para el negocio en vez de corregirlo.

### Tablero

```bash
docker compose --profile dashboard up --build
# → http://localhost:8501
```

Seis pestañas: **Resumen ejecutivo** (vigentes, cambios, alertas,
reconciliación) · **Evolución temporal** · **Análisis financiero** (por fondo,
producto, comercio y tipo) · **Calidad de datos** (rechazos, avisos, cuarentena
navegable) · **Explorador de cambios** (ocho filtros, valores anteriores y
nuevos columna a columna) · **Eventos a revisar**.

La base se abre en solo lectura: el tablero no puede alterar el resultado del
pipeline.

---

## 12. Pruebas

```bash
docker compose run --rm tests
```

**183 pruebas, todas en verde dentro de Docker.**

| Área | Pruebas |
| --- | ---: |
| Validación de archivo, esquema, registro y normalización | 57 |
| Normalización: trazabilidad de valores originales | 16 |
| Detección de cambios | 15 |
| Persistencia e histórico | 13 |
| Reconciliación | 10 |
| Idempotencia | 10 |
| Pipeline, CLI, guardas y extremo a extremo (incluye 2 con datos reales) | 10 |
| Separador de sentencias SQL | 10 |
| Duplicados | 8 |
| Ejes y presentación del tablero | 7 |
| Retención del histórico | 6 |
| Transaccionalidad, rollback y frontera poscommit | 7 |
| Publicación de reportes | 1 |
| Tablero (humo, requiere Streamlit) | 5 |
| Denominador del % de bajas | 3 |
| Trazabilidad del contrato de datos | 3 |
| La clave de negocio del contrato gobierna el particionado y el hash | 2 |

Las pruebas usan datos sintéticos pequeños y controlados; 2 (en
`test_pipeline.py`) están marcadas `realdata` y verifican invariantes globales
sobre los archivos de Tyba. Localmente (`python -m pytest`) son 178: las 5 de
humo del tablero se saltan si Streamlit no está instalado.

```bash
python -m pytest                      # todo
python -m pytest -m "not realdata"    # sin los archivos reales
python -m pytest -k idempotency -v
```

---

## 13. Escalabilidad

### Medido

| Volumen | Filas totales | Tiempo | Filas/s | Base |
| --- | ---: | ---: | ---: | ---: |
| **Datos reales** | 99.000 | **2,34 s** | 42.308 | 87,31 MB |
| Sintético 250k/corte | 495.000 | 6,56 s | 75.485 | 328 MB |
| Sintético 1M/corte | 1.980.000 | 22,61 s | 87.556 | 1.242 MB |
| **Sintético 7M/corte** | **13.860.000** | **252,71 s** | **54.845** | **10,1 GB** |
| **Sintético 10M/corte** | **19.800.000** | **418,67 s** | **47.293** | **14,38 GB** |

El caso de 10 millones necesita 8 GB de memoria para DuckDB, 4 hilos y
alrededor de 12 GB asignados a Docker: con menos margen, la conversión de
importes fuera de rango puede fallar en vez de ir a cuarentena. Reproducible
con datos sintéticos propios, sin tocar los archivos de Tyba:

```bash
docker compose run --rm benchmark --rows 500000 2000000
TYBA_MEMORY_LIMIT=6GB docker compose run --rm benchmark --rows 7000000
TYBA_MEMORY_LIMIT=8GB TYBA_THREADS=4 docker compose run --rm benchmark --rows 10000000
```

### Cuello de botella

La persistencia es el 69% del tiempo a escala: el `UPDATE movements_history
WHERE movement_key = ? AND is_current` necesita el índice
`idx_history_current` (`sql/schema.sql`).

El SHA-256 por fila es un 11% adicional. Sustituirlo por xxHash implica
cambiar `sql/staging.sql`, versionar el hash y reconstruir las claves — no es
un cambio de configuración y no es el cuello de botella principal.

### Cuándo migrar

| Señal | Migrar a |
| --- | --- |
| Varios procesos escriben a la vez | PostgreSQL |
| Más de 10 usuarios consultando en concurrencia | PostgreSQL |
| Estado vigente > 50 GB | Delta Lake / Iceberg |
| Corte diario > 100M filas | Spark / Databricks |
| Alta disponibilidad o control de acceso | PostgreSQL gestionado |

Pasar de 50.000 a 5 millones de filas por corte **no** justifica migrar. La
lógica de negocio sobrevive a la migración porque la identidad y las reglas de
calidad viven en `config/data_contract.yml`, no en el dialecto SQL.

---

## 14. Decisiones técnicas

11 ADR completos, con las alternativas descartadas y su umbral de
reconsideración, en [`docs/decisions.md`](docs/decisions.md):

| Decisión | Motivo |
| --- | --- |
| **DuckDB**, no PostgreSQL | Batch diario mono-escritor cuya operación central es un `FULL OUTER JOIN`. La ventaja de PostgreSQL es concurrencia multi-escritor, que aquí no existe |
| **DECIMAL(20,2)**, no DOUBLE | Importes financieros agregados sobre decenas de miles de filas: con `DOUBLE` el error se acumula |
| **Sin Spark, Kafka, Airflow ni K8s** | El arranque de Spark supera el tiempo total del pipeline; Kafka resuelve streaming y esto es batch; el pipeline ya es idempotente y devuelve códigos de salida estándar, así que se integra con un orquestador sin cambios el día que exista |
| **Capas lógicas**, no directorios | Silver y Gold son tablas dentro de la base (ADR-003); Parquet intermedios romperían la atomicidad del bloque Gold |

**Limitaciones asumidas.** Un solo proceso escritor. Sin replicación, alta
disponibilidad ni control de acceso por usuario. El archivo crece
monolíticamente.

---

## 15. Supuestos y limitaciones

Lo que el enunciado no define y hubo que decidir, con su evidencia, su riesgo y
cómo revertirlo, en
[`docs/analysis_and_assumptions.md`](docs/analysis_and_assumptions.md).

| # | Ambigüedad | Decisión | Reversible en |
| --- | --- | --- | --- |
| A-01 | No existe identificador de transacción | Clave de negocio derivada + ordinal | `data_contract.yml → identity.business_key` |
| A-02 | Misma clave con datos distintos | Ordinal determinista + marca (comportamiento fijo) | `sql/staging.sql`, no configurable |
| A-03 | ¿Los cortes son completos? | Sí, con guardas que lo verifican | `pipeline.yml → guards` |
| A-04 | ¿Un movimiento sin importe es válido? | No: va a cuarentena | `pipeline.yml → null_amount_policy` |
| A-05 | ¿`amount` debe ser positivo? | No se asume nada; se reporta el hallazgo | – |
| A-06 | ¿Cuántas bajas son demasiadas? | Aviso al 10%, fallo al 30% | `pipeline.yml → guards` |
| A-07 | Fecha de corte de los archivos entregados | 2024-10-15 y 2024-10-16, por `max(date)` | `pipeline.yml → declared_sequence` |
| A-08 | ¿`date` es fecha de movimiento o de actualización? | De movimiento: nunca cambia entre versiones | – |
| A-09 | ¿Un fondo o producto nuevo es un error? | No: se conserva y se marca | `data_contract.yml → unknown_value_policy` |

### Limitaciones conocidas

1. **La clave de negocio es un supuesto**, no un dato del origen. Validada
   contra los archivos entregados, pero un identificador real sería más
   robusto.
2. **El ordinal de desempate puede reasignarse** si cambia el orden relativo
   de los importes dentro de un grupo ambiguo. Afecta como máximo a 264
   movimientos (0,55% del vigente), todos marcados con `is_key_ambiguous`.
3. **Solo hay dos cortes.** Las métricas de tendencia y los umbrales basados
   en ventanas históricas necesitan más cortes para ser significativos.
4. **Un 22% de bajas diarias es alto** para un feed financiero real. Se
   reporta como anomalía; es una característica del dataset del ejercicio.
5. **Los insights describen datos aparentemente sintéticos**: distribuciones
   planas en todas las dimensiones.

### Preguntas para el negocio

1. ¿Cuál es el identificador real de una transacción?
2. ¿Qué significa un importe negativo en un movimiento de entrada?
3. ¿Es normal que desaparezca el 22% de los movimientos de un día para otro?
4. ¿Qué se espera de un movimiento sin importe?
5. ¿Los cortes son siempre completos?
6. ¿Un movimiento eliminado puede reaparecer legítimamente?

---

## 16. Solución de problemas

### Códigos de salida

| Código | Significado | Qué mirar |
| ---: | --- | --- |
| 1 | Error crítico: archivo ausente, ilegible, vacío, sin una columna obligatoria o con demasiados rechazos | `tail -50 data/reports/pipeline.log` |
| 2 | Reconciliación fallida. Los cambios se revirtieron, la base está intacta | `SELECT * FROM reconciliation_results WHERE NOT passed` |
| 3 | Guarda disparada. No se aplicó ningún cambio; el corte parece truncado | `SELECT * FROM run_alerts WHERE severity='CRITICAL'` |

Las consultas se lanzan con `docker compose run --rm shell`. Si un corte que
dispara una guarda es legítimo, el umbral se ajusta en `config/pipeline.yml`
(`guards:`) o puntualmente por entorno:

```bash
TYBA_GUARDS__MAX_DELETED_PCT__FAIL=0.5 docker compose up
```

### Situaciones frecuentes

| Síntoma | Causa y solución |
| --- | --- |
| `docker compose up` termina y no aparece el tablero | Es lo esperado: el perfil por defecto ejecuta y termina. Usa `--profile dashboard` |
| El pipeline dice `SKIPPED` | La idempotencia funcionando: ya se procesaron. `make reset`, o `--force` para reprocesar |
| `not a valid DuckDB database file` | Ejecución interrumpida. El pipeline lo detecta y lo borra al arrancar; basta con volver a ejecutar. El tablero, al ser solo lectura, nunca borra: lo explica y se detiene |
| `NoSessionContext: Cursor is not set` | Se lanzó con `python dashboard/app.py`. Streamlit necesita su runtime: `streamlit run dashboard/app.py` |
| `MISSING_REQUIRED_COLUMN` | Falta una de las 8 columnas del contrato. `docker compose run --rm profile` y revisa `schema_comparison.csv` |
| Muchos registros en cuarentena | ~3% por importes nulos es lo esperado. Para conservarlos: `quality.null_amount_policy: keep_with_warning` |
| El tablero dice que no existe la base | Ejecuta antes el pipeline; con `--profile dashboard` es automático |
| Conflictos de permisos en `data/` | El contenedor corre como uid 10001: `sudo chown -R $(id -u):$(id -g) data/` |
| El puerto 8501 está ocupado | Cambia el mapeo en `docker-compose.yml`: `"8600:8501"` |

Nada de esto pierde datos: el estado se reconstruye entero desde `data/raw/`.

---

## Documentación adicional

| Documento | Contenido |
| --- | --- |
| [`docs/analysis_and_assumptions.md`](docs/analysis_and_assumptions.md) | Cómo se interpretó el enunciado, hallazgos sobre los datos, 9 supuestos con evidencia, ambigüedades, riesgos |
| [`docs/architecture.md`](docs/architecture.md) | Diagrama, capas, módulos, flujo, tratamiento de errores |
| [`docs/data_model.md`](docs/data_model.md) | Diccionario de tablas, vistas, invariantes, consultas |
| [`docs/decisions.md`](docs/decisions.md) | 11 ADR con alternativas descartadas |
