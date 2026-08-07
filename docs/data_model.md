# Modelo de datos

Base: `data/database/movements.duckdb` (DuckDB). Todos los importes son
`DECIMAL(20,2)`; nunca `DOUBLE`.

---

## 1. Diagrama de relaciones

```
   file_registry                    pipeline_runs
   PK (source_file_hash,            PK (run_id)
       snapshot_date)                    │
        │                                ├──< run_alerts
        │                                ├──< reconciliation_results
        │                                └──< anomalies
        │                                     │
        └─────────────┬───────────────────────┘
                      │ run_id
                      ▼
              stg_movements  (transitoria, se vacía por run_id)
                      │
        ┌─────────────┼──────────────────┐
        ▼             ▼                  ▼
 rejected_records  data_quality_flags   movements_current
                                        PK (movement_key)
                                              │
                        ┌─────────────────────┼──────────────────┐
                        ▼                     ▼                  │
              movements_history        movement_changes           │
              PK (movement_key,        PK (change_id)             │
                  version)                   │                    │
                                             ▼                    │
                                    movement_change_fields ───────┘
```

---

## 2. `movements_current` - estado vigente

Una fila por movimiento. **La `PRIMARY KEY` garantiza a nivel de motor la
invariante "como máximo una fila vigente por movimiento"**: no depende de que el
código sea correcto.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `movement_key` | VARCHAR **PK** | SHA-256 de la clave de negocio + ordinal |
| `id_cliente` | VARCHAR | Identificador del cliente (columna real del origen) |
| `movement_date` | DATE | Fecha del movimiento, ya parseada |
| `product` | VARCHAR | Producto canonicalizado |
| `type` | VARCHAR | `IN` o `OUT` |
| `fund` | VARCHAR | Fondo canonicalizado |
| `amount` | DECIMAL(20,2) | Importe exacto |
| `description` | VARCHAR | Puede ser nulo |
| `commercial_name` | VARCHAR | Puede ser nulo |
| `occurrence_ordinal` | INTEGER | Desempate dentro de la clave de negocio |
| `is_key_ambiguous` | BOOLEAN | `true` si la clave se repetía en el corte |
| `row_hash` | VARCHAR | SHA-256 del contenido completo; es lo que decide UPDATED |
| `version` | INTEGER | Versión vigente en el histórico |
| `is_active` | BOOLEAN | `false` = dado de baja (nunca se borra físicamente) |
| `first_seen_at` / `first_snapshot_date` | TIMESTAMP / DATE | Primer corte en que apareció |
| `last_seen_at` / `last_snapshot_date` | TIMESTAMP / DATE | Último corte en que se vio |
| `updated_at` | TIMESTAMP | Última modificación de contenido |
| `deleted_at` | TIMESTAMP | Momento de la baja lógica |
| `source_file` / `source_file_hash` / `run_id` | VARCHAR | Trazabilidad al origen |

**Sin cambios** solo actualiza `last_seen_at` y `last_snapshot_date`. El
contenido, la versión y el histórico no se tocan.

---

## 3. `movements_history` - histórico SCD tipo 2

Una fila por **versión del contenido**. `PRIMARY KEY (movement_key, version)`.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `movement_key` | VARCHAR **PK** | |
| `version` | INTEGER **PK** | Consecutiva sin huecos por `movement_key`; empieza en 1 salvo que la retención (`persistence.history_retention_days`) haya purgado las versiones cerradas más antiguas |
| *(columnas de negocio)* | | Foto del contenido en esa versión |
| `row_hash` | VARCHAR | Hash del contenido de esta versión |
| `change_type` | VARCHAR | Evento que **creó** esta versión: `NEW`, `UPDATED`, `REACTIVATED` |
| `valid_from` | DATE | Corte desde el que rige |
| `valid_to` | DATE | Corte en que dejó de regir. `NULL` mientras es la vigente |
| `is_current` | BOOLEAN | Exactamente una por `movement_key` |
| `is_deleted` | BOOLEAN | `true` si la versión se cerró por una **baja** |
| `snapshot_date` / `closed_by_run_id` | DATE / VARCHAR | Trazabilidad |
| `source_file` / `source_file_hash` / `run_id` / `created_at` | | Trazabilidad |

**Decisión relevante.** Una baja **no crea una versión vacía**: cierra la versión
vigente con `valid_to`, `is_current = false` e `is_deleted = true`. Una fila
fantasma sin datos ensuciaría cualquier agregación sobre el histórico.

---

## 4. `movement_changes` - bitácora de eventos

Cabecera del cambio. Una fila por evento real (`NEW`, `UPDATED`, `DELETED`,
`REACTIVATED`). Los `UNCHANGED` **no** generan evento.

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `change_id` | BIGINT **PK** | Secuencia |
| `run_id` / `snapshot_date` / `movement_key` | | Trazabilidad |
| `change_type` | VARCHAR | Tipo de evento |
| `from_version` / `to_version` | INTEGER | Versiones implicadas |
| `changed_columns` | VARCHAR[] | Columnas que cambiaron (solo en `UPDATED`) |
| `amount_before` / `amount_after` | DECIMAL(20,2) | Importes |
| `amount_delta` | DECIMAL(20,2) | Impacto monetario sobre el estado vigente |
| `detected_at` | TIMESTAMP | |

`amount_delta` se define de forma que **suma exactamente la variación del monto
vigente**: alta `+importe`, corrección `nuevo − anterior`, baja `−importe`. Es
uno de los controles de reconciliación.

---

## 5. `movement_change_fields` - detalle columna a columna

Estructura **relacional**, no JSON: se puede agregar, filtrar y unir con SQL
plano. Responde a "¿qué columnas se corrigen más a menudo?" sin parsear nada.

| Columna | Tipo |
| --- | --- |
| `change_id` | BIGINT |
| `run_id` / `movement_key` | VARCHAR |
| `column_name` | VARCHAR |
| `old_value` / `new_value` | VARCHAR |

---

## 6. `rejected_records` - cuarentena

Ningún registro inválido desaparece. La fila **original completa** se conserva en
JSON, sin normalizar.

| Columna | Tipo |
| --- | --- |
| `run_id` / `source_file` / `source_file_hash` / `snapshot_date` | Trazabilidad |
| `source_row_number` | BIGINT |
| `id_cliente` | VARCHAR |
| `error_code` / `error_description` / `error_severity` | VARCHAR |
| `raw_record` | VARCHAR (JSON) |
| `rejected_at` | TIMESTAMP |

---

## 7. Tablas de control

| Tabla | Contenido |
| --- | --- |
| `file_registry` | Un registro por `(contenido, corte)`. Base de la idempotencia. |
| `pipeline_runs` | Auditoría por ejecución: estado, conteos, importes, duración, error, `contract_version` y `contract_hash`. Única fuente de métricas por ejecución. |
| `run_alerts` | Alertas emitidas con su umbral y el valor observado. |
| `reconciliation_results` | Los 12 controles de cuadre con izquierda, derecha, diferencia y resultado. |
| `data_quality_flags` | Avisos no bloqueantes por registro. |
| `anomalies` | Eventos que merecen revisión, con método y umbral. |
| `stg_movements` | Zona de trabajo. Se vacía por `run_id` al inicio de cada ejecución. |

`contract_hash` es el SHA-256 de los bytes exactos de
`config/data_contract.yml` leídos al arrancar. Junto con `contract_version`
permite identificar qué contrato produjo cada ejecución, incluso si el YAML se
modifica posteriormente. Las bases existentes reciben ambas columnas mediante
una migración idempotente al inicializar el esquema.

---

## 8. Vistas analíticas

| Vista | Responde a |
| --- | --- |
| `v_movements_active` | Movimientos vigentes (base de casi todo lo demás) |
| `v_daily_movements` | Distribución por fecha del movimiento |
| `v_daily_changes` | Qué cambios trajo cada corte y su impacto monetario |
| `v_snapshot_summary` | Una fila por corte procesado con éxito |
| `v_summary_by_fund` / `_product` / `_commercial` / `_type` | Cortes financieros por dimensión |
| `v_change_detail` | Explorador de cambios del tablero |
| `v_change_fields` | Valores antiguos y nuevos por columna |
| `v_field_change_frequency` | Qué columnas se corrigen más |
| `v_quality_by_run` | Tasa de validez y de rechazo por corte |
| `v_rejections_by_code` | Motivos de rechazo |
| `v_quality_flags_summary` | Avisos agregados |
| `v_top_movements` | Movimientos de mayor importe |
| `v_top_changes_by_impact` | Correcciones y bajas de mayor impacto |

Son vistas, no tablas materializadas: a este volumen recalcularlas es
despreciable y evita una fuente más de inconsistencia. Si el volumen lo
exigiera, el cambio es `CREATE TABLE ... AS` al final del pipeline, sin tocar
nada más.

---

## 9. Consultas de ejemplo

```sql
-- Estado vigente y balance
SELECT count(*)                                      AS movimientos,
       sum(amount) FILTER (WHERE type = 'IN')        AS entradas,
       sum(amount) FILTER (WHERE type = 'OUT')       AS salidas,
       sum(amount) FILTER (WHERE type = 'IN')
         - sum(amount) FILTER (WHERE type = 'OUT')   AS balance_neto
FROM movements_current WHERE is_active;

-- Historia completa de un movimiento
SELECT version, amount, description, change_type, valid_from, valid_to,
       is_current, is_deleted
FROM movements_history
WHERE movement_key = '75fd41f8e65b134e…'
ORDER BY version;

-- Correcciones de un corte, con el valor anterior y el nuevo
SELECT mc.movement_key, f.column_name, f.old_value, f.new_value, mc.amount_delta
FROM movement_changes mc
JOIN movement_change_fields f USING (change_id)
WHERE mc.change_type = 'UPDATED' AND mc.snapshot_date = DATE '2024-10-16'
ORDER BY abs(mc.amount_delta) DESC
LIMIT 20;

-- Movimientos dados de baja y cuánto dinero se llevaron
SELECT c.id_cliente, c.fund, c.product, c.amount, c.deleted_at
FROM movements_current c
WHERE NOT c.is_active
ORDER BY c.amount DESC;

-- ¿Cuadró todo en la última ejecución?
SELECT check_group, check_name, left_value, right_value, difference, passed
FROM reconciliation_results
WHERE run_id = (SELECT run_id FROM pipeline_runs
                WHERE status = 'SUCCESS' ORDER BY started_at DESC LIMIT 1)
ORDER BY passed, check_group;

-- Registros en cuarentena, con su fila original
SELECT snapshot_date, error_code, count(*) AS filas,
       any_value(raw_record)                AS ejemplo
FROM rejected_records GROUP BY 1, 2 ORDER BY 3 DESC;
```

---

## 10. Invariantes verificadas en cada ejecución

`src/persistence.py::check_invariants` las comprueba **dentro de la
transacción**. Cualquier violación provoca `ROLLBACK`.

| Invariante | Qué evita |
| --- | --- |
| Como máximo una fila vigente por `movement_key` | Duplicación del estado |
| Como máximo una versión con `is_current` por `movement_key` | Histórico ambiguo |
| Todo movimiento vigente tiene al menos una versión histórica | Estado sin trazabilidad |
| Las versiones son consecutivas, sin huecos | Pérdida de versiones |
| Un movimiento activo apunta a una versión abierta | Incoherencia entre estado e histórico |
| Toda corrección tiene su detalle de columnas modificadas | Cambios sin auditoría |
