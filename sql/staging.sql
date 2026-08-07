-- =============================================================================
-- STAGING: tipado, normalizacion, validacion, deduplicacion e identidad
-- =============================================================================
-- Todo el trabajo por fila se hace aqui, en SQL sobre conjuntos, leyendo el
-- parquet directamente: sin bucles de Python ni el dataset entero en memoria.
--
-- Los marcadores entre llaves los sustituye src/normalization.py desde la
-- configuracion (expresiones, dominios, separador de hash, escala decimal).
--
-- Parametros con nombre: path, run_id, source_file, source_file_hash,
--                        snapshot_date, ingestion_ts
-- =============================================================================

CREATE OR REPLACE TEMP TABLE tmp_normalized AS
WITH raw AS (
    SELECT
        row_number() OVER () AS source_row_number,
        CAST(id_cliente      AS VARCHAR) AS id_cliente_raw,
        CAST(date            AS VARCHAR) AS date_raw,
        CAST(product         AS VARCHAR) AS product_raw,
        CAST(type            AS VARCHAR) AS type_raw,
        CAST(fund            AS VARCHAR) AS fund_raw,
        CAST(amount          AS VARCHAR) AS amount_text,
        TRY_CAST(amount      AS DOUBLE)  AS amount_raw,
        CAST(description     AS VARCHAR) AS description_raw,
        CAST(commercial_name AS VARCHAR) AS commercial_name_raw
    FROM read_parquet($path)
),
normalized AS (
    SELECT
        source_row_number,
        id_cliente_raw, date_raw, product_raw, type_raw, fund_raw, amount_text,
        amount_raw, description_raw, commercial_name_raw,

        -- id: solo se recortan espacios externos; el contenido no se altera.
        nullif(trim(id_cliente_raw), '')                                   AS id_cliente,
        -- date: parseo multi-formato. Devuelve NULL si ningun formato aplica.
        {date_expr}                                                        AS movement_date,
        -- product / fund: trim + colapso de espacios internos + catalogo canonico.
        {product_expr}                                                     AS product,
        {fund_expr}                                                        AS fund,
        -- type: mapeo a IN/OUT. NULL si el valor no esta en el catalogo de sinonimos.
        {type_expr}                                                        AS type,
        -- amount: DOUBLE de origen -> DECIMAL exacto. NaN/Inf se descartan antes del cast.
        CASE
            WHEN amount_raw IS NULL THEN NULL
            WHEN isnan(amount_raw) OR isinf(amount_raw) THEN NULL
            ELSE TRY_CAST(amount_raw AS {amount_type})
        END                                                                AS amount,
        -- texto libre: solo espacios. La cadena vacia se normaliza a NULL.
        nullif(trim(description_raw), '')                                  AS description,
        nullif(regexp_replace(trim(commercial_name_raw), '\s+', ' ', 'g'), '') AS commercial_name,

        (amount_raw IS NOT NULL AND (isnan(amount_raw) OR isinf(amount_raw)))   AS is_non_finite,
        (amount_raw IS NOT NULL AND NOT isnan(amount_raw) AND NOT isinf(amount_raw)
            AND abs(amount_raw * pow(10, {scale}) - round(amount_raw * pow(10, {scale}))) > 1e-6
        )                                                                       AS has_extra_decimals
    FROM raw
),
classified AS (
    SELECT
        n.*,
        -- ---------------------------------------------------------------------
        -- Primer error de registro que invalida la fila (orden determinista).
        -- NULL => la fila es apta para el estado vigente.
        -- ---------------------------------------------------------------------
        CASE
            WHEN id_cliente_raw IS NULL                       THEN 'NULL_ID'
            WHEN id_cliente IS NULL                           THEN 'EMPTY_ID'
            WHEN date_raw IS NULL                             THEN 'NULL_DATE'
            WHEN movement_date IS NULL                        THEN 'INVALID_DATE'
            WHEN product IS NULL                              THEN 'NULL_PRODUCT'
            WHEN type_raw IS NULL                             THEN 'NULL_TYPE'
            WHEN type IS NULL                                 THEN 'UNKNOWN_TYPE'
            WHEN fund IS NULL                                 THEN 'NULL_FUND'
            WHEN amount_text IS NULL AND {reject_null_amount} THEN 'NULL_AMOUNT'
            WHEN amount_text IS NOT NULL AND amount_raw IS NULL
                                                               THEN 'INVALID_AMOUNT_FORMAT'
            WHEN is_non_finite                                THEN 'NON_FINITE_AMOUNT'
            WHEN amount_raw IS NOT NULL AND amount IS NULL    THEN 'AMOUNT_OUT_OF_RANGE'
        END AS error_code,
        -- ---------------------------------------------------------------------
        -- Flags no bloqueantes. Se conservan para cuantificar la calidad sin
        -- perder el registro.
        -- ---------------------------------------------------------------------
        list_filter([
            CASE WHEN id_cliente IS NOT NULL AND NOT regexp_matches(id_cliente, '^CLI[0-9]{6}$')
                 THEN 'INVALID_ID_FORMAT' END,
            CASE WHEN product IS NOT NULL AND product NOT IN {product_domain}
                 THEN 'UNKNOWN_PRODUCT' END,
            CASE WHEN fund IS NOT NULL AND fund NOT IN {fund_domain}
                 THEN 'UNKNOWN_FUND' END,
            CASE WHEN movement_date IS NOT NULL AND movement_date > CAST($snapshot_date AS DATE) + INTERVAL 1 DAY
                 THEN 'DATE_OUT_OF_RANGE' END,
            CASE WHEN has_extra_decimals              THEN 'AMOUNT_PRECISION_LOSS' END,
            CASE WHEN amount IS NOT NULL AND amount < 0 THEN 'NEGATIVE_AMOUNT' END,
            CASE WHEN amount IS NOT NULL AND amount = 0 THEN 'ZERO_AMOUNT' END,
            CASE WHEN amount IS NULL AND NOT {reject_null_amount} THEN 'NULL_AMOUNT_KEPT' END,
            CASE WHEN description IS NULL             THEN 'NULL_DESCRIPTION' END,
            CASE WHEN commercial_name IS NULL         THEN 'NULL_COMMERCIAL_NAME' END
        ], x -> x IS NOT NULL) AS quality_flags
    FROM normalized n
)
SELECT * FROM classified;


-- -----------------------------------------------------------------------------
-- 1. CUARENTENA: los registros invalidos NO desaparecen.
--    Se guarda la fila ORIGINAL completa en JSON, sin normalizar.
-- -----------------------------------------------------------------------------
INSERT INTO rejected_records
    (run_id, source_file, source_file_hash, snapshot_date, source_row_number,
     id_cliente, error_code, error_description, error_severity, raw_record)
SELECT
    $run_id, $source_file, $source_file_hash, CAST($snapshot_date AS DATE), source_row_number,
    id_cliente_raw,
    error_code,
    CASE error_code
        WHEN 'NULL_ID'            THEN 'id_cliente nulo'
        WHEN 'EMPTY_ID'           THEN 'id_cliente vacio tras recortar espacios'
        WHEN 'NULL_DATE'          THEN 'date nula'
        WHEN 'INVALID_DATE'       THEN 'date no parseable con los formatos aceptados'
        WHEN 'NULL_PRODUCT'       THEN 'product nulo'
        WHEN 'NULL_TYPE'          THEN 'type nulo'
        WHEN 'UNKNOWN_TYPE'       THEN 'type no mapeable a IN/OUT'
        WHEN 'NULL_FUND'          THEN 'fund nulo'
        WHEN 'NON_FINITE_AMOUNT'  THEN 'amount NaN o infinito'
        WHEN 'INVALID_AMOUNT_FORMAT' THEN 'amount no se puede convertir al formato numerico canonico'
        WHEN 'AMOUNT_OUT_OF_RANGE' THEN 'amount excede la precision decimal configurada'
        WHEN 'NULL_AMOUNT'        THEN 'amount nulo: movimiento no reconciliable financieramente'
        ELSE error_code
    END,
    'RECORD_ERROR',
    to_json({
        'id_cliente':      id_cliente_raw,
        'date':            date_raw,
        'product':         product_raw,
        'type':            type_raw,
        'fund':            fund_raw,
        'amount':          amount_text,
        'description':     description_raw,
        'commercial_name': commercial_name_raw
    })
FROM tmp_normalized
WHERE error_code IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 2. DEDUPLICACION EXACTA + IDENTIDAD + HASH DE FILA
--    - Duplicado exacto (misma fila normalizada completa): se conserva una y se
--      contabiliza en exact_duplicate_count.
--    - occurrence_ordinal desempata movimientos legitimamente repetidos que
--      comparten la clave de negocio.
-- -----------------------------------------------------------------------------
INSERT INTO stg_movements
SELECT
    $run_id, $source_file, $source_file_hash, CAST($snapshot_date AS DATE),
    CAST($ingestion_ts AS TIMESTAMP), source_row_number,

    -- movement_key: SHA-256 sobre la serializacion estable de la clave de
    -- negocio declarada en identity.business_key (config/data_contract.yml)
    -- + el ordinal de ocurrencia. Orden fijo, nulos explicitos, fecha ISO.
    sha256(concat_ws({sep},
        '{hash_version}',
        {business_key_hash_parts},
        CAST(occurrence_ordinal AS VARCHAR)
    ))                                                              AS movement_key,
    occurrence_ordinal,
    is_key_ambiguous,

    id_cliente, movement_date, product, type, fund, amount, description, commercial_name,
    date_raw, product_raw, type_raw, fund_raw, amount_raw, description_raw, commercial_name_raw,

    -- row_hash: SHA-256 sobre TODAS las columnas de negocio, incluidas las de la
    -- clave. Serializacion estable: separador fijo, token explicito de nulo,
    -- fecha ISO y decimal con escala fija (CAST de DECIMAL a VARCHAR es exacto).
    sha256(concat_ws({sep},
        '{hash_version}',
        coalesce(id_cliente, {null_token}),
        coalesce(strftime(movement_date, '%Y-%m-%d'), {null_token}),
        coalesce(product, {null_token}),
        coalesce(type, {null_token}),
        coalesce(fund, {null_token}),
        coalesce(CAST(amount AS VARCHAR), {null_token}),
        coalesce(description, {null_token}),
        coalesce(commercial_name, {null_token})
    ))                                                              AS row_hash,
    exact_duplicate_count,
    CASE WHEN len(quality_flags) = 0 THEN 'VALID' ELSE 'WARN' END   AS validation_status,
    CASE WHEN is_key_ambiguous
         THEN list_append(quality_flags, 'KEY_AMBIGUOUS')
         ELSE quality_flags END                                     AS quality_flags
FROM (
    SELECT
        d.*,
        -- Particion por la clave de negocio del contrato (identity.business_key).
        row_number() OVER (
            PARTITION BY {business_key_columns}
            ORDER BY amount NULLS LAST, description NULLS LAST,
                     commercial_name NULLS LAST, source_row_number
        )                                                           AS occurrence_ordinal,
        count(*) OVER (
            PARTITION BY {business_key_columns}
        ) > 1                                                       AS is_key_ambiguous
    FROM (
        SELECT
            min(source_row_number)                                  AS source_row_number,
            id_cliente, movement_date, product, type, fund, amount, description, commercial_name,
            any_value(date_raw)            AS date_raw,
            any_value(product_raw)         AS product_raw,
            any_value(type_raw)            AS type_raw,
            any_value(fund_raw)            AS fund_raw,
            any_value(amount_raw)          AS amount_raw,
            any_value(description_raw)     AS description_raw,
            any_value(commercial_name_raw) AS commercial_name_raw,
            CAST(count(*) AS INTEGER)      AS exact_duplicate_count,
            any_value(quality_flags)       AS quality_flags
        FROM tmp_normalized
        WHERE error_code IS NULL
        GROUP BY id_cliente, movement_date, product, type, fund, amount, description, commercial_name
    ) d
) x;


-- -----------------------------------------------------------------------------
-- 3. FLAGS DE CALIDAD (INFO / WARNING) -> tabla propia, consultable y agregable.
-- -----------------------------------------------------------------------------
INSERT INTO data_quality_flags (run_id, snapshot_date, movement_key, flag_code, severity, column_name, detail)
SELECT
    $run_id, CAST($snapshot_date AS DATE), s.movement_key, f.flag_code,
    CASE f.flag_code
        WHEN 'NEGATIVE_AMOUNT'         THEN 'INFO'
        WHEN 'ZERO_AMOUNT'             THEN 'INFO'
        WHEN 'NULL_DESCRIPTION'        THEN 'INFO'
        WHEN 'NULL_COMMERCIAL_NAME'    THEN 'INFO'
        ELSE 'WARNING'
    END,
    CASE f.flag_code
        WHEN 'INVALID_ID_FORMAT'      THEN 'id_cliente'
        WHEN 'UNKNOWN_PRODUCT'        THEN 'product'
        WHEN 'UNKNOWN_FUND'           THEN 'fund'
        WHEN 'DATE_OUT_OF_RANGE'      THEN 'date'
        WHEN 'AMOUNT_PRECISION_LOSS'  THEN 'amount'
        WHEN 'NEGATIVE_AMOUNT'        THEN 'amount'
        WHEN 'ZERO_AMOUNT'            THEN 'amount'
        WHEN 'NULL_AMOUNT_KEPT'       THEN 'amount'
        WHEN 'NULL_DESCRIPTION'       THEN 'description'
        WHEN 'NULL_COMMERCIAL_NAME'   THEN 'commercial_name'
        WHEN 'KEY_AMBIGUOUS'          THEN 'movement_key'
    END,
    NULL
FROM stg_movements s, UNNEST(s.quality_flags) AS f(flag_code)
WHERE s.run_id = $run_id;
