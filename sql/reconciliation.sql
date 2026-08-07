-- =============================================================================
-- RECONCILIACION
-- =============================================================================
-- Cada control compara dos magnitudes que DEBEN coincidir. Se ejecuta despues
-- de aplicar los cambios, dentro de la misma ejecucion, y su resultado queda
-- persistido en reconciliation_results.
--
-- Los montos se comparan como DECIMAL, nunca como coma flotante, por lo que la
-- tolerancia por defecto es 0 (igualdad exacta).
--
-- Parametros con nombre: run_id, snapshot_date, rows_read, active_before,
--                        amount_before
-- =============================================================================

INSERT INTO reconciliation_results
    (run_id, snapshot_date, check_group, check_name, left_label, left_value,
     right_label, right_value, difference, tolerance, passed, detail)

-- -----------------------------------------------------------------------------
-- G1. Conservacion de filas en la ingesta
--     filas leidas = filas validas (incluidos duplicados exactos) + rechazadas
-- -----------------------------------------------------------------------------
WITH stg AS (
    SELECT
        coalesce(sum(exact_duplicate_count), 0) AS rows_valid_expanded,
        count(*)                                AS rows_valid_distinct
    FROM stg_movements WHERE run_id = $run_id
),
rej AS (
    SELECT count(*) AS rows_rejected FROM rejected_records WHERE run_id = $run_id
),
chg AS (
    SELECT
        count(*)                                                    AS total_keys,
        count(*) FILTER (WHERE change_type = 'NEW')                 AS n_new,
        count(*) FILTER (WHERE change_type = 'UPDATED')             AS n_upd,
        count(*) FILTER (WHERE change_type = 'DELETED')             AS n_del,
        count(*) FILTER (WHERE change_type = 'UNCHANGED')           AS n_unc,
        count(*) FILTER (WHERE change_type = 'REACTIVATED')         AS n_rea,
        count(*) FILTER (WHERE change_type = 'STILL_DELETED')       AS n_std
    FROM tmp_changes
),
cur AS (
    SELECT
        count(*) FILTER (WHERE is_active)                  AS active_after,
        coalesce(sum(amount) FILTER (WHERE is_active), 0)  AS amount_after
    FROM movements_current
),
delta AS (
    SELECT coalesce(sum(amount_delta), 0) AS amount_delta
    FROM movement_changes WHERE run_id = $run_id
),
checks AS (
    SELECT 'INGESTA' AS g, 'filas_leidas = validas + rechazadas' AS c,
           'rows_read' AS ll, CAST($rows_read AS DECIMAL(38,4)) AS lv,
           'valid+rejected' AS rl,
           CAST(stg.rows_valid_expanded + rej.rows_rejected AS DECIMAL(38,4)) AS rv,
           NULL AS detail
    FROM stg, rej

    UNION ALL
    -- -------------------------------------------------------------------------
    -- G2. Clasificacion completa: ningun id se pierde ni se cuenta dos veces
    -- -------------------------------------------------------------------------
    SELECT 'CAMBIOS', 'new+updated+deleted+unchanged+reactivated+ya_dados_de_baja = claves del FULL OUTER JOIN',
           'suma_por_tipo', CAST(n_new + n_upd + n_del + n_unc + n_rea + n_std AS DECIMAL(38,4)),
           'claves_join',   CAST(total_keys AS DECIMAL(38,4)), NULL
    FROM chg

    UNION ALL
    SELECT 'CAMBIOS', 'new+updated+unchanged+reactivated = claves distintas del corte',
           'clasificadas_del_corte', CAST(n_new + n_upd + n_unc + n_rea AS DECIMAL(38,4)),
           'staging_distintas',      CAST(stg.rows_valid_distinct AS DECIMAL(38,4)), NULL
    FROM chg, stg

    UNION ALL
    -- -------------------------------------------------------------------------
    -- G3. Continuidad del estado vigente
    -- -------------------------------------------------------------------------
    SELECT 'ESTADO', 'vigentes_despues = vigentes_antes + nuevos + reactivados - eliminados',
           'esperado', CAST($active_before + n_new + n_rea - n_del AS DECIMAL(38,4)),
           'observado', CAST(cur.active_after AS DECIMAL(38,4)), NULL
    FROM chg, cur

    UNION ALL
    SELECT 'ESTADO', 'todas las claves del corte estan en el estado vigente y activas',
           'sin_reflejo', CAST((
               SELECT count(*) FROM stg_movements s
               WHERE s.run_id = $run_id
                 AND NOT EXISTS (SELECT 1 FROM movements_current m
                                 WHERE m.movement_key = s.movement_key AND m.is_active)
           ) AS DECIMAL(38,4)),
           'cero', CAST(0 AS DECIMAL(38,4)), NULL

    UNION ALL
    SELECT 'ESTADO', 'como maximo una fila vigente por movement_key',
           'claves_duplicadas', CAST((
               SELECT count(*) FROM (SELECT movement_key FROM movements_current
                                     GROUP BY 1 HAVING count(*) > 1)
           ) AS DECIMAL(38,4)),
           'cero', CAST(0 AS DECIMAL(38,4)), NULL

    UNION ALL
    SELECT 'HISTORICO', 'como maximo una version marcada como vigente por movement_key',
           'versiones_vigentes_duplicadas', CAST((
               SELECT count(*) FROM (SELECT movement_key FROM movements_history
                                     WHERE is_current GROUP BY 1 HAVING count(*) > 1)
           ) AS DECIMAL(38,4)),
           'cero', CAST(0 AS DECIMAL(38,4)), NULL

    UNION ALL
    -- La purga de historico (src/persistence.py::prune_history) puede dejar la
    -- secuencia empezando en una version > 1, pero nunca con huecos: se compara
    -- contra (max - min + 1), no contra max.
    SELECT 'HISTORICO', 'versiones consecutivas sin huecos',
           'claves_con_hueco', CAST((
               SELECT count(*) FROM (SELECT movement_key,
                                            max(version) - min(version) + 1 AS esperado,
                                            count(*) AS n
                                     FROM movements_history GROUP BY 1 HAVING esperado <> n)
           ) AS DECIMAL(38,4)),
           'cero', CAST(0 AS DECIMAL(38,4)), NULL

    UNION ALL
    -- -------------------------------------------------------------------------
    -- G4. Continuidad monetaria: el monto vigente se mueve exactamente lo que
    --     suman los impactos monetarios registrados en la bitacora.
    -- -------------------------------------------------------------------------
    SELECT 'MONETARIA', 'monto_vigente_despues = monto_vigente_antes + suma(impacto de los cambios)',
           'esperado',  CAST(CAST($amount_before AS DECIMAL(38,4)) + delta.amount_delta AS DECIMAL(38,4)),
           'observado', CAST(cur.amount_after AS DECIMAL(38,4)), NULL
    FROM delta, cur

    UNION ALL
    SELECT 'MONETARIA', 'suma de montos del corte = suma de montos de sus claves en el estado vigente',
           'staging', CAST((SELECT coalesce(sum(amount), 0) FROM stg_movements WHERE run_id = $run_id) AS DECIMAL(38,4)),
           'vigente', CAST((
               SELECT coalesce(sum(m.amount), 0) FROM movements_current m
               WHERE EXISTS (SELECT 1 FROM stg_movements s
                             WHERE s.run_id = $run_id AND s.movement_key = m.movement_key)
           ) AS DECIMAL(38,4)), NULL

    UNION ALL
    -- -------------------------------------------------------------------------
    -- G5. Bitacora completa
    -- -------------------------------------------------------------------------
    SELECT 'BITACORA', 'eventos registrados = cambios reales detectados',
           'movement_changes', CAST((SELECT count(*) FROM movement_changes WHERE run_id = $run_id) AS DECIMAL(38,4)),
           'cambios_reales',   CAST(n_new + n_upd + n_del + n_rea AS DECIMAL(38,4)), NULL
    FROM chg

    UNION ALL
    SELECT 'BITACORA', 'toda correccion tiene al menos una columna modificada registrada',
           'correcciones_sin_detalle', CAST((
               SELECT count(*) FROM movement_changes mc
               WHERE mc.run_id = $run_id AND mc.change_type = 'UPDATED'
                 AND NOT EXISTS (SELECT 1 FROM movement_change_fields f
                                 WHERE f.change_id = mc.change_id)
           ) AS DECIMAL(38,4)),
           'cero', CAST(0 AS DECIMAL(38,4)), NULL
)
SELECT
    $run_id, CAST($snapshot_date AS DATE), g, c, ll, lv, rl, rv,
    lv - rv AS difference,
    CAST(0 AS DECIMAL(38,4)) AS tolerance,
    (lv - rv) = 0 AS passed,
    detail
FROM checks;
