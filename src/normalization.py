"""Construccion de las expresiones de normalizacion y ejecucion de la capa staging.

Las reglas NO estan escritas a mano en el SQL: se generan desde
config/data_contract.yml y config/pipeline.yml. Cambiar un sinonimo de `type` o
anadir un formato de fecha es un cambio de configuracion, no de codigo.
"""

from __future__ import annotations

from datetime import date, datetime

from src.config import Config
from src.database import Database
from src.logging_config import get_logger

log = get_logger("normalization")


def _q(value: str) -> str:
    """Literal SQL entrecomillado, con escape de comillas simples."""
    return "'" + str(value).replace("'", "''") + "'"


def _in_list(values: list[str]) -> str:
    return "(" + ", ".join(_q(v) for v in values) + ")" if values else "('__none__')"


def sql_text_expr(value: str) -> str:
    """Convierte una cadena en una expresion SQL segura.

    Los caracteres de control (los separadores de hash US/RS) se emiten como
    chr(N) en vez de como literales: incrustarlos en crudo es fragil.
    """
    parts: list[str] = []
    buf: list[str] = []
    for ch in value:
        if ord(ch) < 32 or ord(ch) == 127:
            if buf:
                parts.append(_q("".join(buf)))
                buf = []
            parts.append(f"chr({ord(ch)})")
        else:
            buf.append(ch)
    if buf:
        parts.append(_q("".join(buf)))
    return " || ".join(parts) if parts else "''"


# --------------------------------------------------------------- expresiones
def date_expression(cfg: Config, column: str = "date_raw") -> str:
    """CASE multi-formato. Devuelve NULL si ningun formato configurado aplica.

    Los dos formatos por defecto estan OBSERVADOS en los archivos entregados:
    ISO (YYYY-MM-DD) y DD/MM/YYYY conviviendo en el mismo archivo.
    """
    parts = []
    for fmt in cfg.get("normalization.accepted_date_formats", []):
        parts.append(
            f"WHEN regexp_matches(trim({column}), {_q(fmt['regex'])}) "
            f"THEN TRY_CAST(try_strptime(trim({column}), {_q(fmt['strptime'])}) AS DATE)")
    if not parts:
        return f"TRY_CAST({column} AS DATE)"
    return "CASE " + " ".join(parts) + " END"


def type_expression(cfg: Config, column: str = "type_raw") -> str:
    """Mapeo de sinonimos a IN/OUT. Un valor fuera del catalogo produce NULL,
    que la capa de validacion convierte en UNKNOWN_TYPE (cuarentena)."""
    whens = []
    for canonical, synonyms in cfg.type_synonyms.items():
        vals = _in_list([s.lower() for s in synonyms])
        whens.append(f"WHEN lower(trim({column})) IN {vals} THEN {_q(canonical)}")
    return "CASE " + " ".join(whens) + " END"


def _catalog_expression(cfg: Config, colname: str, column: str,
                        case_insensitive: bool) -> str:
    """trim + colapso de espacios + (opcional) canonicalizacion de mayusculas.

    Si el valor no esta en el catalogo se CONSERVA tal cual (normalizado): un
    fondo o producto nuevo es una situacion de negocio legitima, no un error.
    Se marca con UNKNOWN_FUND / UNKNOWN_PRODUCT y se reporta como anomalia.
    """
    cleaned = f"nullif(regexp_replace(trim({column}), '\\s+', ' ', 'g'), '')"
    values = cfg.domain_values(colname)
    if not values:
        return cleaned
    whens = []
    for v in values:
        if case_insensitive:
            whens.append(f"WHEN lower({cleaned}) = {_q(v.lower())} THEN {_q(v)}")
        else:
            whens.append(f"WHEN {cleaned} = {_q(v)} THEN {_q(v)}")
    return "CASE " + " ".join(whens) + f" ELSE {cleaned} END"


def product_expression(cfg: Config, column: str = "product_raw") -> str:
    # Los productos llegan limpios en los archivos observados; aun asi se
    # canonicaliza de forma case-insensitive por robustez ante cortes futuros.
    return _catalog_expression(cfg, "product", column, case_insensitive=True)


def fund_expression(cfg: Config, column: str = "fund_raw") -> str:
    # fund SI llega sucio: mayusculas, espacios externos y dobles espacios.
    return _catalog_expression(cfg, "fund", column, case_insensitive=True)


# Nombre de columna en el contrato -> columna normalizada real en tmp_normalized.
# Solo `date` difiere: se renombra a `movement_date` para no chocar con la
# funcion SQL homonima.
_NORMALIZED_COLUMN = {"date": "movement_date"}


def business_key_columns(cfg: Config) -> list[str]:
    """Columnas normalizadas (tmp_normalized/stg_movements) que forman la
    clave de negocio declarada en identity.business_key del contrato."""
    return [_NORMALIZED_COLUMN.get(name, name) for name in cfg.business_key]


def business_key_partition_expr(cfg: Config) -> str:
    """Lista para PARTITION BY: que fila es 'la misma' segun el contrato."""
    return ", ".join(business_key_columns(cfg))


def business_key_hash_parts_expr(cfg: Config) -> str:
    """Fragmento de argumentos de concat_ws para movement_key: una columna
    por elemento de business_key, serializada igual que en row_hash (fecha
    ISO, token de nulo explicito)."""
    null_token = sql_text_expr(cfg.null_token)
    parts = []
    for col in business_key_columns(cfg):
        if col == "movement_date":
            parts.append(f"coalesce(strftime(movement_date, '%Y-%m-%d'), {null_token})")
        else:
            parts.append(f"coalesce({col}, {null_token})")
    return ",\n        ".join(parts)


# ------------------------------------------------------------------ staging
def build_staging(db: Database, cfg: Config, *, path: str, run_id: str,
                  source_file: str, source_file_hash: str,
                  snapshot_date: date, ingestion_ts: datetime | None = None) -> dict:
    """Materializa stg_movements + rejected_records + data_quality_flags.

    Devuelve las metricas de la capa de staging.
    """
    ingestion_ts = ingestion_ts or datetime.utcnow()
    reject_null_amount = cfg.get("quality.null_amount_policy", "quarantine") == "quarantine"

    sql = db.sql_file(
        "sql/staging.sql",
        date_expr=date_expression(cfg),
        type_expr=type_expression(cfg),
        product_expr=product_expression(cfg),
        fund_expr=fund_expression(cfg),
        product_domain=_in_list(cfg.domain_values("product")),
        fund_domain=_in_list(cfg.domain_values("fund")),
        reject_null_amount="TRUE" if reject_null_amount else "FALSE",
        sep=sql_text_expr(cfg.field_separator),
        null_token=sql_text_expr(cfg.null_token),
        hash_version=cfg.hash_version,
        scale=cfg.amount_scale,
        business_key_columns=business_key_partition_expr(cfg),
        business_key_hash_parts=business_key_hash_parts_expr(cfg),
    )

    params = {
        "path": path,
        "run_id": run_id,
        "source_file": source_file,
        "source_file_hash": source_file_hash,
        "snapshot_date": snapshot_date.isoformat(),
        "ingestion_ts": ingestion_ts.isoformat(sep=" ", timespec="seconds"),
    }

    # Limpieza de la zona de trabajo: staging es transitoria por ejecucion.
    db.execute("DELETE FROM stg_movements WHERE run_id = ?", [run_id])
    db.execute("DELETE FROM rejected_records WHERE run_id = ?", [run_id])
    db.execute("DELETE FROM data_quality_flags WHERE run_id = ?", [run_id])

    # DuckDB exige que el diccionario de parametros coincida EXACTAMENTE con los
    # que aparecen en la sentencia: se filtra por sentencia.
    for statement in split_statements(sql):
        used = {k: v for k, v in params.items() if f"${k}" in statement}
        db.con.execute(statement, used) if used else db.con.execute(statement)

    metrics = db.execute(
        """
        SELECT
            (SELECT count(*) FROM stg_movements    WHERE run_id = $run_id)                        AS rows_valid,
            (SELECT count(*) FROM rejected_records WHERE run_id = $run_id)                        AS rows_rejected,
            (SELECT coalesce(sum(exact_duplicate_count - 1), 0) FROM stg_movements WHERE run_id = $run_id) AS rows_exact_dupes,
            (SELECT count(*) FROM stg_movements    WHERE run_id = $run_id AND is_key_ambiguous)   AS rows_key_ambiguous,
            (SELECT coalesce(sum(amount), 0) FROM stg_movements WHERE run_id = $run_id AND type = 'IN')  AS amount_in,
            (SELECT coalesce(sum(amount), 0) FROM stg_movements WHERE run_id = $run_id AND type = 'OUT') AS amount_out
        """, {"run_id": run_id}).df().iloc[0].to_dict()

    log.info("Staging listo: %s validas | %s rechazadas | %s duplicados exactos colapsados "
             "| %s con clave ambigua",
             f"{int(metrics['rows_valid']):,}", f"{int(metrics['rows_rejected']):,}",
             f"{int(metrics['rows_exact_dupes']):,}", f"{int(metrics['rows_key_ambiguous']):,}")
    return metrics


def split_statements(sql: str) -> list[str]:
    """Separa un script SQL en sentencias.

    Respeta literales entre comillas simples, identificadores entre comillas
    dobles y comentarios de linea (--) y de bloque, de forma que un ';' dentro
    de un comentario o de una cadena no parta la sentencia.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    in_single = in_double = in_line_comment = in_block_comment = False

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
        elif in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 1
                in_block_comment = False
        elif in_single:
            buf.append(ch)
            if ch == "'":
                if nxt == "'":          # comilla escapada ''
                    buf.append(nxt)
                    i += 1
                else:
                    in_single = False
        elif in_double:
            buf.append(ch)
            if ch == '"':
                in_double = False
        elif ch == "-" and nxt == "-":
            in_line_comment = True
            buf.append(ch)
        elif ch == "/" and nxt == "*":
            in_block_comment = True
            buf.append(ch)
        elif ch == "'":
            in_single = True
            buf.append(ch)
        elif ch == '"':
            in_double = True
            buf.append(ch)
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1

    statements.append("".join(buf))
    return [s.strip() for s in statements if _has_code(s)]


def _has_code(chunk: str) -> bool:
    """True si el fragmento contiene algo mas que comentarios y espacios."""
    for line in chunk.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False
