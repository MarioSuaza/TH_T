-- =============================================================================
-- DETECCION DE CAMBIOS ENTRE EL CORTE ENTRANTE Y EL ESTADO VIGENTE
-- =============================================================================
-- Compara contra movements_current (estado acumulado), no contra el archivo
-- anterior: por eso T+2, T+3... funcionan sin cambios. El contenido se compara
-- por row_hash. Un FULL OUTER JOIN sobre movement_key, resuelto con hash join.
--
-- Marcadores sustituidos por src/change_detection.py.
-- Parametros con nombre: run_id, snapshot_date, reactivation_type
-- =============================================================================

CREATE OR REPLACE TEMP TABLE tmp_changes AS
WITH incoming AS (
    SELECT
        movement_key, occurrence_ordinal, is_key_ambiguous,
        id_cliente, movement_date, product, type, fund,
        amount, description, commercial_name,
        date_original, type_original, amount_original,
        row_hash, source_file, source_file_hash, ingestion_timestamp
    FROM stg_movements
    WHERE run_id = $run_id
),
cur AS (
    SELECT
        movement_key, version, is_active, row_hash,
        id_cliente        AS cur_id_cliente,
        movement_date     AS cur_movement_date,
        product           AS cur_product,
        type              AS cur_type,
        fund              AS cur_fund,
        amount            AS cur_amount,
        description       AS cur_description,
        commercial_name   AS cur_commercial_name,
        first_seen_at, first_snapshot_date
    FROM movements_current
)
SELECT
    coalesce(i.movement_key, c.movement_key)                          AS movement_key,

    -- -------------------------------------------------------------------------
    -- Clasificacion. El orden de los CASE es significativo.
    -- -------------------------------------------------------------------------
    CASE
        -- Nunca visto: alta.
        WHEN c.movement_key IS NULL                       THEN 'NEW'
        -- Estaba vigente y ya no llega: baja logica.
        WHEN i.movement_key IS NULL AND c.is_active       THEN 'DELETED'
        -- Ya estaba dado de baja y sigue sin llegar: no es un cambio.
        WHEN i.movement_key IS NULL                       THEN 'STILL_DELETED'
        -- Estaba dado de baja y vuelve a aparecer.
        WHEN NOT c.is_active                              THEN '{reactivation_type}'
        -- Existe en ambos: el contenido decide.
        WHEN i.row_hash IS DISTINCT FROM c.row_hash       THEN 'UPDATED'
        ELSE 'UNCHANGED'
    END                                                               AS change_type,

    c.version                                                         AS old_version,
    coalesce(c.version, 0) + 1                                        AS new_version,
    c.is_active                                                       AS was_active,

    -- Valores entrantes
    i.id_cliente, i.movement_date, i.product, i.type, i.fund,
    i.amount, i.description, i.commercial_name,
    i.date_original, i.type_original, i.amount_original,
    i.occurrence_ordinal, i.is_key_ambiguous,
    i.row_hash                                                        AS new_row_hash,
    i.source_file, i.source_file_hash, i.ingestion_timestamp,

    -- Valores vigentes previos
    c.cur_id_cliente, c.cur_movement_date, c.cur_product, c.cur_type, c.cur_fund,
    c.cur_amount, c.cur_description, c.cur_commercial_name,
    c.row_hash                                                        AS old_row_hash,
    c.first_seen_at, c.first_snapshot_date,

    -- -------------------------------------------------------------------------
    -- Columnas modificadas. IS DISTINCT FROM trata NULL como un valor mas:
    -- NULL vs NULL no es un cambio; NULL vs valor si lo es.
    -- -------------------------------------------------------------------------
    CASE
        WHEN c.movement_key IS NULL OR i.movement_key IS NULL THEN NULL
        ELSE list_filter([{changed_columns_expr}], x -> x IS NOT NULL)
    END                                                               AS changed_columns,

    CAST($snapshot_date AS DATE)                                      AS snapshot_date
FROM incoming i
FULL OUTER JOIN cur c ON i.movement_key = c.movement_key;
