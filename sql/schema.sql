-- =============================================================================
-- ESQUEMA DE PERSISTENCIA (DuckDB)
-- =============================================================================
-- {amount_type} lo sustituye src/database.py a partir de config/pipeline.yml
-- (normalization.amount.precision / .scale). Nunca se usa DOUBLE para montos
-- persistidos.
--
-- Todo el DDL es idempotente: se puede ejecutar en cada arranque sin efecto.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE / AUDITORIA DE ARCHIVOS
-- -----------------------------------------------------------------------------
-- Un registro por (contenido, corte). La unicidad se establece sobre el hash del
-- contenido y la fecha del corte, no sobre el nombre del archivo.
--
-- La fecha forma parte de la clave porque el mismo contenido en un corte
-- POSTERIOR es una reversion legitima del origen y si debe aplicarse; repetirlo
-- en el MISMO corte es un no-op. Solo con el hash se ignorarian las reversiones.
CREATE TABLE IF NOT EXISTS file_registry (
    source_file_hash   VARCHAR      NOT NULL,
    source_file        VARCHAR      NOT NULL,
    source_path        VARCHAR      NOT NULL,
    size_bytes         BIGINT       NOT NULL,
    file_mtime         TIMESTAMP,
    snapshot_date      DATE,
    row_count          BIGINT,
    column_names       VARCHAR[],
    schema_fingerprint VARCHAR,
    first_seen_at      TIMESTAMP    NOT NULL DEFAULT now(),
    processed_at       TIMESTAMP,
    status             VARCHAR      NOT NULL,   -- DISCOVERED|PROCESSING|PROCESSED|FAILED|SKIPPED|REJECTED
    run_id             VARCHAR,
    error_code         VARCHAR,
    error_message      VARCHAR,
    PRIMARY KEY (source_file_hash, snapshot_date)
);

-- -----------------------------------------------------------------------------
-- AUDITORIA DE EJECUCIONES
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id             VARCHAR      NOT NULL,
    started_at         TIMESTAMP    NOT NULL,
    finished_at        TIMESTAMP,
    status             VARCHAR      NOT NULL,   -- RUNNING|SUCCESS|SKIPPED|FAILED|FAILED_GUARD
    snapshot_date      DATE,
    input_file         VARCHAR,
    input_hash         VARCHAR,
    rows_read          BIGINT DEFAULT 0,
    rows_exact_dupes   BIGINT DEFAULT 0,
    rows_valid         BIGINT DEFAULT 0,
    rows_rejected      BIGINT DEFAULT 0,
    rows_new           BIGINT DEFAULT 0,
    rows_updated       BIGINT DEFAULT 0,
    rows_deleted       BIGINT DEFAULT 0,
    rows_unchanged     BIGINT DEFAULT 0,
    rows_reactivated   BIGINT DEFAULT 0,
    -- Vigentes ANTES de aplicar el corte. Es el denominador de toda metrica
    -- expresada "sobre el vigente previo": sin persistirlo, analytics no puede
    -- reconstruirlo y termina usando una aproximacion incorrecta.
    rows_current_before BIGINT DEFAULT 0,
    rows_current_after BIGINT DEFAULT 0,
    amount_in          {amount_type},
    amount_out         {amount_type},
    amount_current     {amount_type},
    duration_seconds   DOUBLE,
    pipeline_version   VARCHAR,
    contract_version   VARCHAR,
    contract_hash      VARCHAR,
    error_code         VARCHAR,
    error_message      VARCHAR,
    PRIMARY KEY (run_id)
);

-- Migracion idempotente para bases creadas antes de que el pipeline registrara
-- la version y la huella exacta del contrato utilizado por cada ejecucion.
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS contract_version VARCHAR;
ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS contract_hash VARCHAR;

-- Alertas logicas emitidas durante la ejecucion.
CREATE TABLE IF NOT EXISTS run_alerts (
    run_id      VARCHAR   NOT NULL,
    alert_code  VARCHAR   NOT NULL,
    severity    VARCHAR   NOT NULL,   -- INFO|WARNING|CRITICAL
    message     VARCHAR   NOT NULL,
    observed    DOUBLE,
    threshold   DOUBLE,
    raised_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- SILVER / STAGING
-- -----------------------------------------------------------------------------
-- Contenido tipado, normalizado y validado del corte en proceso. Se vacia al
-- inicio de cada ejecucion: es una zona de trabajo, no un almacen.
CREATE TABLE IF NOT EXISTS stg_movements (
    run_id                  VARCHAR      NOT NULL,
    source_file             VARCHAR      NOT NULL,
    source_file_hash        VARCHAR      NOT NULL,
    snapshot_date           DATE         NOT NULL,
    ingestion_timestamp     TIMESTAMP    NOT NULL,
    source_row_number       BIGINT       NOT NULL,

    movement_key            VARCHAR      NOT NULL,
    occurrence_ordinal      INTEGER      NOT NULL,
    is_key_ambiguous        BOOLEAN      NOT NULL DEFAULT FALSE,

    id_cliente              VARCHAR,
    movement_date           DATE,
    product                 VARCHAR,
    type                    VARCHAR,
    fund                    VARCHAR,
    amount                  {amount_type},
    description             VARCHAR,
    commercial_name         VARCHAR,

    -- Valores originales conservados: toda normalizacion es trazable y reversible.
    date_original           VARCHAR,
    product_original        VARCHAR,
    type_original           VARCHAR,
    fund_original           VARCHAR,
    amount_original         DOUBLE,
    description_original    VARCHAR,
    commercial_name_original VARCHAR,

    row_hash                VARCHAR      NOT NULL,
    exact_duplicate_count   INTEGER      NOT NULL DEFAULT 1,
    validation_status       VARCHAR      NOT NULL,   -- VALID|WARN
    quality_flags           VARCHAR[]
);

-- -----------------------------------------------------------------------------
-- CUARENTENA
-- -----------------------------------------------------------------------------
-- Ningun registro invalido desaparece: queda aqui con su fila cruda completa.
CREATE TABLE IF NOT EXISTS rejected_records (
    run_id            VARCHAR   NOT NULL,
    source_file       VARCHAR   NOT NULL,
    source_file_hash  VARCHAR   NOT NULL,
    snapshot_date     DATE,
    source_row_number BIGINT,
    id_cliente        VARCHAR,
    error_code        VARCHAR   NOT NULL,
    error_description VARCHAR,
    error_severity    VARCHAR   NOT NULL,
    raw_record        VARCHAR   NOT NULL,   -- JSON de la fila original sin modificar
    rejected_at       TIMESTAMP NOT NULL DEFAULT now()
);

-- Flags de calidad no bloqueantes (INFO/WARNING). Se conservan para poder
-- cuantificar la calidad sin rechazar el registro.
CREATE TABLE IF NOT EXISTS data_quality_flags (
    run_id        VARCHAR NOT NULL,
    snapshot_date DATE,
    movement_key  VARCHAR,
    flag_code     VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL,
    column_name   VARCHAR,
    detail        VARCHAR
);

-- -----------------------------------------------------------------------------
-- GOLD / ESTADO VIGENTE
-- -----------------------------------------------------------------------------
-- PRIMARY KEY garantiza a nivel de motor "como maximo una fila por movimiento".
CREATE TABLE IF NOT EXISTS movements_current (
    movement_key        VARCHAR   NOT NULL,
    id_cliente          VARCHAR   NOT NULL,
    movement_date       DATE      NOT NULL,
    product             VARCHAR   NOT NULL,
    type                VARCHAR   NOT NULL,
    fund                VARCHAR   NOT NULL,
    amount              {amount_type},
    description         VARCHAR,
    commercial_name     VARCHAR,

    -- Valores tal como llegaron del archivo. Sin ellos no es posible auditar
    -- que produjo la normalizacion ni demostrar que 'entrada'/'IN'/'ENTRADA'
    -- convergen al mismo valor canonico sin generar falsas bajas.
    date_original       VARCHAR,
    type_original       VARCHAR,
    amount_original     DOUBLE,

    occurrence_ordinal  INTEGER   NOT NULL,
    is_key_ambiguous    BOOLEAN   NOT NULL DEFAULT FALSE,
    row_hash            VARCHAR   NOT NULL,
    version             INTEGER   NOT NULL,
    is_active           BOOLEAN   NOT NULL,
    first_seen_at       TIMESTAMP NOT NULL,
    first_snapshot_date DATE      NOT NULL,
    last_seen_at        TIMESTAMP NOT NULL,
    last_snapshot_date  DATE      NOT NULL,
    updated_at          TIMESTAMP NOT NULL,
    deleted_at          TIMESTAMP,
    source_file         VARCHAR   NOT NULL,
    source_file_hash    VARCHAR   NOT NULL,
    run_id              VARCHAR   NOT NULL,
    PRIMARY KEY (movement_key)
);

-- -----------------------------------------------------------------------------
-- GOLD / HISTORICO (SCD tipo 2)
-- -----------------------------------------------------------------------------
-- Una fila por version del contenido del movimiento.
-- La eliminacion NO crea una version vacia: cierra la version vigente
-- (valid_to = snapshot_date, is_current = FALSE, is_deleted = TRUE).
CREATE TABLE IF NOT EXISTS movements_history (
    movement_key      VARCHAR   NOT NULL,
    version           INTEGER   NOT NULL,
    id_cliente        VARCHAR   NOT NULL,
    movement_date     DATE      NOT NULL,
    product           VARCHAR   NOT NULL,
    type              VARCHAR   NOT NULL,
    fund              VARCHAR   NOT NULL,
    amount            {amount_type},
    description       VARCHAR,
    commercial_name   VARCHAR,
    row_hash          VARCHAR   NOT NULL,
    change_type       VARCHAR   NOT NULL,   -- evento que CREO esta version: NEW|UPDATED|REACTIVATED
    valid_from        DATE      NOT NULL,
    valid_to          DATE,                 -- NULL mientras es la version vigente
    is_current        BOOLEAN   NOT NULL,
    is_deleted        BOOLEAN   NOT NULL DEFAULT FALSE,
    snapshot_date     DATE      NOT NULL,
    closed_by_run_id  VARCHAR,
    source_file       VARCHAR   NOT NULL,
    source_file_hash  VARCHAR   NOT NULL,
    run_id            VARCHAR   NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (movement_key, version)
);

-- -----------------------------------------------------------------------------
-- GOLD / BITACORA DE CAMBIOS
-- -----------------------------------------------------------------------------
-- Cabecera del evento. El detalle columna a columna vive en movement_change_fields
-- (estructura relacional, consultable con SQL plano; no JSON).
CREATE TABLE IF NOT EXISTS movement_changes (
    change_id       BIGINT    NOT NULL,
    run_id          VARCHAR   NOT NULL,
    snapshot_date   DATE      NOT NULL,
    movement_key    VARCHAR   NOT NULL,
    change_type     VARCHAR   NOT NULL,   -- NEW|UPDATED|DELETED|REACTIVATED
    from_version    INTEGER,
    to_version      INTEGER,
    changed_columns VARCHAR[],
    amount_before   {amount_type},
    amount_after    {amount_type},
    amount_delta    {amount_type},
    detected_at     TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (change_id)
);

CREATE TABLE IF NOT EXISTS movement_change_fields (
    change_id     BIGINT  NOT NULL,
    run_id        VARCHAR NOT NULL,
    movement_key  VARCHAR NOT NULL,
    column_name   VARCHAR NOT NULL,
    old_value     VARCHAR,
    new_value     VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS seq_change_id START 1;

-- -----------------------------------------------------------------------------
-- RECONCILIACION
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconciliation_results (
    run_id       VARCHAR   NOT NULL,
    snapshot_date DATE,
    check_group  VARCHAR   NOT NULL,
    check_name   VARCHAR   NOT NULL,
    left_label   VARCHAR,
    left_value   DECIMAL(38,4),
    right_label  VARCHAR,
    right_value  DECIMAL(38,4),
    difference   DECIMAL(38,4),
    tolerance    DECIMAL(38,4) DEFAULT 0,
    passed       BOOLEAN   NOT NULL,
    detail       VARCHAR,
    checked_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- ANOMALIAS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    run_id        VARCHAR   NOT NULL,
    snapshot_date DATE,
    anomaly_code  VARCHAR   NOT NULL,
    category      VARCHAR   NOT NULL,   -- QUALITY|CHANGE|OUTLIER|REVIEW|BUSINESS_RULE
    severity      VARCHAR   NOT NULL,
    entity_type   VARCHAR,              -- movement|fund|product|commercial_name|snapshot|column
    entity_id     VARCHAR,
    metric_name   VARCHAR,
    observed      DOUBLE,
    expected      DOUBLE,
    threshold     DOUBLE,
    description   VARCHAR   NOT NULL,
    detected_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- INDICES
-- -----------------------------------------------------------------------------
-- El cierre de versiones filtra por (movement_key, is_current), no por la
-- PRIMARY KEY (movement_key, version). Sin este indice cada corte recorre el
-- historico completo y su coste pasa a depender del acumulado, no del corte.
-- DuckDB no soporta indices parciales: se indexa movement_key primero, que es
-- la columna del join.
CREATE INDEX IF NOT EXISTS idx_history_current
    ON movements_history (movement_key, is_current);

-- La bitacora se agrega por corte en analitica y reportes.
CREATE INDEX IF NOT EXISTS idx_changes_snapshot
    ON movement_changes (snapshot_date, change_type);

-- La cuarentena se agrupa por ejecucion y codigo en cada informe de calidad.
CREATE INDEX IF NOT EXISTS idx_rejected_run
    ON rejected_records (run_id, error_code);
