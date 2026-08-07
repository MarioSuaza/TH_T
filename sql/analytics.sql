-- =============================================================================
-- CAPA ANALITICA (GOLD)
-- =============================================================================
-- Vistas, no tablas materializadas: sobre DuckDB el coste de recalcular estas
-- agregaciones es despreciable frente al de mantenerlas sincronizadas, y evita
-- una fuente adicional de inconsistencia. Si el volumen creciera hasta hacerlas
-- caras, el cambio es CREATE TABLE ... AS al final del pipeline, sin tocar el
-- resto (ver docs/scalability.md).
-- =============================================================================

-- Movimientos vigentes: es la vista de partida de casi todo lo demas.
CREATE OR REPLACE VIEW v_movements_active AS
SELECT * FROM movements_current WHERE is_active;

-- -----------------------------------------------------------------------------
-- Evolucion temporal por FECHA DEL MOVIMIENTO (no por fecha de corte)
-- -----------------------------------------------------------------------------
-- `is_partial` distingue los dias que NO son comparables con el resto de la
-- serie: el ultimo dia observado suele estar en curso cuando se toma el corte,
-- asi que su volumen es una fraccion del de un dia cerrado. Dibujado sin marca
-- parece una caida del negocio y no lo es (ver docs/ANOMALY_2024_10_16.md).
--
-- El criterio es derivado, no una lista fija de fechas: es el ultimo dia de la
-- serie Y ademas su volumen cae por debajo de la mitad de la mediana. Los dos
-- requisitos juntos: un ultimo dia con volumen normal no se marca, y un valle
-- en mitad de la serie (un domingo, p.ej.) tampoco.
CREATE OR REPLACE VIEW v_daily_movements AS
WITH por_dia AS (
    SELECT
        movement_date,
        count(*)                                                   AS movements,
        count(DISTINCT id_cliente)                                 AS clients,
        coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)        AS amount_in,
        coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0)       AS amount_out,
        coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)
          - coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0)   AS net_flow,
        count(*) FILTER (WHERE type = 'IN')                        AS movements_in,
        count(*) FILTER (WHERE type = 'OUT')                       AS movements_out,
        coalesce(avg(amount), 0)                                   AS avg_amount,
        coalesce(median(amount), 0)                                AS median_amount
    FROM v_movements_active
    GROUP BY 1
),
referencia AS (
    SELECT median(movements) AS mediana, max(movement_date) AS ultimo FROM por_dia
)
SELECT
    p.*,
    (p.movement_date = r.ultimo AND p.movements < r.mediana * 0.5) AS is_partial
FROM por_dia p, referencia r
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- Evolucion por CORTE: cuantos cambios trajo cada snapshot
-- -----------------------------------------------------------------------------
-- Solo la ultima ejecucion de cada corte: movement_changes acumula los eventos
-- de cada reproceso. El log completo sigue en movement_changes.
CREATE OR REPLACE VIEW v_daily_changes AS
WITH ultima_ejecucion AS (
    SELECT run_id
    FROM pipeline_runs
    WHERE status = 'SUCCESS'
    QUALIFY row_number() OVER (PARTITION BY snapshot_date
                               ORDER BY started_at DESC) = 1
)
SELECT
    c.snapshot_date,
    c.change_type,
    count(*)                                  AS movements,
    coalesce(sum(c.amount_delta), 0)          AS amount_impact,
    coalesce(sum(abs(c.amount_delta)), 0)     AS amount_impact_abs
FROM movement_changes c
JOIN ultima_ejecucion u USING (run_id)
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE OR REPLACE VIEW v_snapshot_summary AS
SELECT
    r.snapshot_date,
    r.run_id,
    r.status,
    r.input_file,
    r.rows_read,
    r.rows_valid,
    r.rows_rejected,
    CASE WHEN r.rows_read > 0
         THEN r.rows_rejected * 1.0 / r.rows_read ELSE 0 END       AS rejection_rate,
    r.rows_new, r.rows_updated, r.rows_deleted, r.rows_unchanged, r.rows_reactivated,
    CASE WHEN (r.rows_new + r.rows_updated + r.rows_unchanged + r.rows_reactivated) > 0
         THEN r.rows_updated * 1.0 / (r.rows_new + r.rows_updated + r.rows_unchanged + r.rows_reactivated)
         ELSE 0 END                                                AS updated_rate,
    r.rows_current_after,
    r.amount_current,
    r.duration_seconds
FROM pipeline_runs r
WHERE r.status = 'SUCCESS'
-- Una fila por corte, no por intento: v_quality_by_run lista cada ejecucion.
QUALIFY row_number() OVER (PARTITION BY r.snapshot_date
                           ORDER BY r.started_at DESC) = 1
ORDER BY r.snapshot_date;

-- -----------------------------------------------------------------------------
-- Cortes financieros por dimension
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_summary_by_fund AS
SELECT fund,
       count(*)                                              AS movements,
       count(DISTINCT id_cliente)                            AS clients,
       coalesce(sum(amount), 0)                              AS amount_total,
       coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)   AS amount_in,
       coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0)  AS amount_out,
       coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)
         - coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0) AS net_flow,
       coalesce(avg(amount), 0)                              AS avg_amount
FROM v_movements_active GROUP BY 1 ORDER BY 4 DESC;

CREATE OR REPLACE VIEW v_summary_by_product AS
SELECT product,
       count(*)                                              AS movements,
       count(DISTINCT id_cliente)                            AS clients,
       coalesce(sum(amount), 0)                              AS amount_total,
       coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)   AS amount_in,
       coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0)  AS amount_out,
       coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)
         - coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0) AS net_flow,
       coalesce(avg(amount), 0)                              AS avg_amount
FROM v_movements_active GROUP BY 1 ORDER BY 4 DESC;

CREATE OR REPLACE VIEW v_summary_by_commercial AS
SELECT coalesce(commercial_name, '(sin nombre comercial)')   AS commercial_name,
       count(*)                                              AS movements,
       count(DISTINCT id_cliente)                            AS clients,
       coalesce(sum(amount), 0)                              AS amount_total,
       coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0)   AS amount_in,
       coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0)  AS amount_out,
       coalesce(avg(amount), 0)                              AS avg_amount
FROM v_movements_active GROUP BY 1 ORDER BY 4 DESC;

CREATE OR REPLACE VIEW v_summary_by_type AS
SELECT type,
       count(*)                     AS movements,
       coalesce(sum(amount), 0)     AS amount_total,
       coalesce(avg(amount), 0)     AS avg_amount,
       coalesce(min(amount), 0)     AS min_amount,
       coalesce(max(amount), 0)     AS max_amount,
       count(*) FILTER (WHERE amount < 0) AS negative_amounts,
       count(*) FILTER (WHERE amount = 0) AS zero_amounts
FROM v_movements_active GROUP BY 1 ORDER BY 3 DESC;

-- -----------------------------------------------------------------------------
-- Detalle de cambios para el explorador del tablero
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_change_detail AS
SELECT
    mc.change_id,
    mc.run_id,
    mc.snapshot_date,
    mc.movement_key,
    mc.change_type,
    mc.from_version,
    mc.to_version,
    mc.changed_columns,
    mc.amount_before,
    mc.amount_after,
    mc.amount_delta,
    mc.detected_at,
    m.id_cliente,
    m.movement_date,
    m.product,
    m.type,
    m.fund,
    m.commercial_name,
    m.is_active,
    m.source_file
FROM movement_changes mc
LEFT JOIN movements_current m ON m.movement_key = mc.movement_key;

CREATE OR REPLACE VIEW v_change_fields AS
SELECT f.change_id, f.movement_key, f.column_name, f.old_value, f.new_value,
       mc.change_type, mc.snapshot_date, mc.run_id
FROM movement_change_fields f
JOIN movement_changes mc ON mc.change_id = f.change_id;

-- Que columnas se corrigen con mas frecuencia
CREATE OR REPLACE VIEW v_field_change_frequency AS
SELECT column_name,
       count(*)                                    AS changes,
       count(DISTINCT movement_key)                AS movements_affected
FROM movement_change_fields
GROUP BY 1 ORDER BY 2 DESC;

-- -----------------------------------------------------------------------------
-- Calidad de datos
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quality_by_run AS
SELECT
    r.run_id,
    r.snapshot_date,
    r.rows_read,
    r.rows_valid,
    r.rows_rejected,
    r.rows_exact_dupes,
    CASE WHEN r.rows_read > 0 THEN r.rows_valid * 1.0 / r.rows_read ELSE 0 END AS valid_rate,
    CASE WHEN r.rows_read > 0 THEN r.rows_rejected * 1.0 / r.rows_read ELSE 0 END AS rejection_rate
FROM pipeline_runs r
WHERE r.status = 'SUCCESS'
ORDER BY r.snapshot_date;

CREATE OR REPLACE VIEW v_rejections_by_code AS
SELECT run_id, snapshot_date, error_code, error_severity, count(*) AS rows_rejected
FROM rejected_records
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;

CREATE OR REPLACE VIEW v_quality_flags_summary AS
SELECT run_id, snapshot_date, flag_code, severity, column_name, count(*) AS occurrences
FROM data_quality_flags
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC;

-- -----------------------------------------------------------------------------
-- Movimientos de mayor importe y correcciones de mayor impacto
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_top_movements AS
SELECT movement_key, id_cliente, movement_date, product, fund, type, amount,
       description, commercial_name
FROM v_movements_active
ORDER BY abs(amount) DESC;

CREATE OR REPLACE VIEW v_top_changes_by_impact AS
SELECT mc.change_id, mc.snapshot_date, mc.change_type, mc.movement_key,
       mc.amount_before, mc.amount_after, mc.amount_delta,
       m.id_cliente, m.fund, m.product, m.type, m.commercial_name
FROM movement_changes mc
LEFT JOIN movements_current m ON m.movement_key = mc.movement_key
ORDER BY abs(mc.amount_delta) DESC NULLS LAST;
