-- =============================================================================
-- PERSISTENCIA: historico (SCD2), estado vigente y bitacora de cambios
-- =============================================================================
-- Todas las sentencias corren dentro de UNA transaccion abierta por
-- src/persistence.py: si una falla, el ROLLBACK deja la base como estaba.
--
-- El orden es significativo:
--   1. cerrar versiones vigentes afectadas
--   2. abrir versiones nuevas
--   3. actualizar el estado vigente
--   4. escribir la bitacora
--
-- Parametros con nombre: run_id, snapshot_date
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Cerrar la version vigente de las claves modificadas o dadas de baja.
--    Una baja NO crea una version vacia: cierra la ultima version y la marca
--    con is_deleted, que es la informacion que realmente aporta valor.
-- -----------------------------------------------------------------------------
UPDATE movements_history AS h
SET valid_to         = CAST($snapshot_date AS DATE),
    is_current       = FALSE,
    is_deleted       = (ch.change_type = 'DELETED'),
    closed_by_run_id = $run_id
FROM tmp_changes ch
WHERE ch.movement_key = h.movement_key
  AND h.is_current
  AND ch.change_type IN ('UPDATED', 'DELETED');

-- -----------------------------------------------------------------------------
-- 2. Abrir una version nueva para altas, correcciones y reactivaciones.
-- -----------------------------------------------------------------------------
INSERT INTO movements_history (
    movement_key, version, id_cliente, movement_date, product, type, fund,
    amount, description, commercial_name, row_hash, change_type,
    valid_from, valid_to, is_current, is_deleted, snapshot_date,
    closed_by_run_id, source_file, source_file_hash, run_id, created_at
)
SELECT
    movement_key, new_version, id_cliente, movement_date, product, type, fund,
    amount, description, commercial_name, new_row_hash, change_type,
    CAST($snapshot_date AS DATE), NULL, TRUE, FALSE, CAST($snapshot_date AS DATE),
    NULL, source_file, source_file_hash, $run_id, now()
FROM tmp_changes
WHERE change_type IN ('NEW', 'UPDATED', 'REACTIVATED');

-- -----------------------------------------------------------------------------
-- 3a. Altas en el estado vigente.
--     La PRIMARY KEY sobre movement_key garantiza, a nivel de motor, que no
--     puede existir mas de una fila por movimiento.
-- -----------------------------------------------------------------------------
INSERT INTO movements_current (
    movement_key, id_cliente, movement_date, product, type, fund,
    amount, description, commercial_name,
    date_original, type_original, amount_original,
    occurrence_ordinal, is_key_ambiguous,
    row_hash, version, is_active, first_seen_at, first_snapshot_date,
    last_seen_at, last_snapshot_date, updated_at, deleted_at,
    source_file, source_file_hash, run_id
)
SELECT
    movement_key, id_cliente, movement_date, product, type, fund,
    amount, description, commercial_name,
    date_original, type_original, amount_original,
    occurrence_ordinal, is_key_ambiguous,
    new_row_hash, new_version, TRUE,
    ingestion_timestamp, CAST($snapshot_date AS DATE),
    ingestion_timestamp, CAST($snapshot_date AS DATE), now(), NULL,
    source_file, source_file_hash, $run_id
FROM tmp_changes
WHERE change_type = 'NEW';

-- -----------------------------------------------------------------------------
-- 3b. Correcciones y reactivaciones: se sustituye el contenido y se conserva
--     first_seen_at / first_snapshot_date (trazabilidad del alta original).
-- -----------------------------------------------------------------------------
UPDATE movements_current AS m
SET id_cliente         = ch.id_cliente,
    movement_date      = ch.movement_date,
    product            = ch.product,
    type               = ch.type,
    fund               = ch.fund,
    amount             = ch.amount,
    description        = ch.description,
    commercial_name    = ch.commercial_name,
    date_original      = ch.date_original,
    type_original      = ch.type_original,
    amount_original    = ch.amount_original,
    occurrence_ordinal = ch.occurrence_ordinal,
    is_key_ambiguous   = ch.is_key_ambiguous,
    row_hash           = ch.new_row_hash,
    version            = ch.new_version,
    is_active          = TRUE,
    last_seen_at       = ch.ingestion_timestamp,
    last_snapshot_date = CAST($snapshot_date AS DATE),
    updated_at         = now(),
    deleted_at         = NULL,
    source_file        = ch.source_file,
    source_file_hash   = ch.source_file_hash,
    run_id             = $run_id
FROM tmp_changes ch
WHERE ch.movement_key = m.movement_key
  AND ch.change_type IN ('UPDATED', 'REACTIVATED');

-- -----------------------------------------------------------------------------
-- 3c. Sin cambios: solo se refresca la marca de "visto por ultima vez".
--     El contenido, la version y el historico NO se tocan.
-- -----------------------------------------------------------------------------
UPDATE movements_current AS m
SET last_seen_at       = ch.ingestion_timestamp,
    last_snapshot_date = CAST($snapshot_date AS DATE)
FROM tmp_changes ch
WHERE ch.movement_key = m.movement_key
  AND ch.change_type = 'UNCHANGED';

-- -----------------------------------------------------------------------------
-- 3d. Bajas: eliminacion LOGICA. La fila permanece y queda auditable.
-- -----------------------------------------------------------------------------
UPDATE movements_current AS m
SET is_active  = FALSE,
    deleted_at = now(),
    updated_at = now(),
    run_id     = $run_id
FROM tmp_changes ch
WHERE ch.movement_key = m.movement_key
  AND ch.change_type = 'DELETED';

-- -----------------------------------------------------------------------------
-- 4. Bitacora de cambios: cabecera del evento + detalle relacional por columna.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE tmp_change_events AS
SELECT nextval('seq_change_id') AS change_id, *
FROM tmp_changes
WHERE change_type IN ('NEW', 'UPDATED', 'DELETED', 'REACTIVATED');

INSERT INTO movement_changes (
    change_id, run_id, snapshot_date, movement_key, change_type,
    from_version, to_version, changed_columns,
    amount_before, amount_after, amount_delta, detected_at
)
SELECT
    change_id, $run_id, CAST($snapshot_date AS DATE), movement_key, change_type,
    old_version,
    CASE WHEN change_type = 'DELETED' THEN old_version ELSE new_version END,
    changed_columns,
    cur_amount,
    CASE WHEN change_type = 'DELETED' THEN NULL ELSE amount END,
    -- Impacto monetario del evento sobre el estado vigente:
    --   alta -> + monto | correccion -> diferencia | baja -> - monto vigente
    -- Cada rama se castea por separado: la resta de dos DECIMAL(p,s) ensancha
    -- el tipo y forzaria un downcast de todo el CASE en el INSERT.
    CASE
        WHEN change_type IN ('NEW', 'REACTIVATED')
            THEN CAST(amount AS {amount_type})
        WHEN change_type = 'UPDATED'
            THEN CAST(coalesce(amount, 0) - coalesce(cur_amount, 0) AS {amount_type})
        WHEN change_type = 'DELETED'
            THEN CAST(-coalesce(cur_amount, 0) AS {amount_type})
    END,
    now()
FROM tmp_change_events;

INSERT INTO movement_change_fields (change_id, run_id, movement_key, column_name, old_value, new_value)
SELECT change_id, $run_id, movement_key, f.column_name, f.old_value, f.new_value
FROM tmp_change_events,
     LATERAL (VALUES
        ('amount',          CAST(cur_amount AS VARCHAR),      CAST(amount AS VARCHAR)),
        ('description',     cur_description,                  description),
        ('commercial_name', cur_commercial_name,              commercial_name)
     ) AS f(column_name, old_value, new_value)
WHERE change_type = 'UPDATED'
  AND f.old_value IS DISTINCT FROM f.new_value;
