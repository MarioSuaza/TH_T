"""Clasificacion de cada movimiento en NEW / UPDATED / DELETED / UNCHANGED
(y REACTIVATED cuando una clave dada de baja reaparece).

La logica vive en sql/change_detection.sql. Este modulo solo construye las
expresiones dependientes de configuracion y expone el recuento resultante.
"""

from __future__ import annotations

from datetime import date

from src.config import Config
from src.database import Database
from src.logging_config import get_logger
from src.normalization import split_statements

log = get_logger("change_detection")

CHANGE_TYPES = ("NEW", "UPDATED", "DELETED", "UNCHANGED", "REACTIVATED", "STILL_DELETED")
# Tipos que representan un cambio real y por tanto se registran en la bitacora.
EVENT_TYPES = ("NEW", "UPDATED", "DELETED", "REACTIVATED")


def changed_columns_expression(cfg: Config) -> str:
    """Lista de columnas cuyo valor difiere entre el corte y el estado vigente.

    Se evalua sobre los atributos mutables del contrato: las columnas de la
    clave de negocio no pueden cambiar por construccion (si cambiaran, seria
    otra movement_key, es decir un DELETED + un NEW).
    """
    parts = []
    for col in cfg.mutable_attributes:
        parts.append(
            f"CASE WHEN i.{col} IS DISTINCT FROM c.cur_{col} THEN '{col}' END")
    return ", ".join(parts)


def detect_changes(db: Database, cfg: Config, *, run_id: str,
                   snapshot_date: date) -> dict:
    """Materializa tmp_changes y devuelve el conteo por tipo de cambio."""
    reactivation = (cfg.get("change_detection.reactivation_change_type", "REACTIVATED")
                    if cfg.get("change_detection.allow_reactivation", True)
                    else "UPDATED")

    sql = db.sql_file(
        "sql/change_detection.sql",
        changed_columns_expr=changed_columns_expression(cfg),
        reactivation_type=reactivation,
    )
    params = {"run_id": run_id, "snapshot_date": snapshot_date.isoformat()}
    for statement in split_statements(sql):
        used = {k: v for k, v in params.items() if f"${k}" in statement}
        db.con.execute(statement, used) if used else db.con.execute(statement)

    counts = dict(db.execute(
        "SELECT change_type, count(*) FROM tmp_changes GROUP BY 1").fetchall())
    for t in CHANGE_TYPES:
        counts.setdefault(t, 0)

    log.info("Cambios detectados: NEW=%s UPDATED=%s DELETED=%s UNCHANGED=%s "
             "REACTIVATED=%s (ya dados de baja: %s)",
             f"{counts['NEW']:,}", f"{counts['UPDATED']:,}", f"{counts['DELETED']:,}",
             f"{counts['UNCHANGED']:,}", f"{counts['REACTIVATED']:,}",
             f"{counts['STILL_DELETED']:,}")
    return counts
