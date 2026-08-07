"""Escritura atomica del historico, el estado vigente y la bitacora de cambios.

Todo ocurre dentro de una unica transaccion. Un fallo intermedio no puede dejar
movements_current actualizado a medias, ni historico incompleto, ni cambios sin
auditar, ni la ejecucion marcada como exitosa.
"""

from __future__ import annotations

from datetime import date

from src.config import Config
from src.database import Database
from src.logging_config import get_logger
from src.normalization import split_statements

log = get_logger("persistence")


PERSISTENCE_STEPS = (
    "cerrar versiones historicas",
    "abrir versiones historicas",
    "insertar altas vigentes",
    "actualizar correcciones y reactivaciones",
    "marcar filas sin cambios",
    "marcar bajas logicas",
    "materializar eventos de cambio",
    "insertar cabeceras de cambio",
    "insertar detalle de campos modificados",
)


def current_state_summary(db: Database) -> dict:
    """Foto del estado vigente: se usa antes y despues para reconciliar."""
    row = db.execute("""
        SELECT
            count(*) FILTER (WHERE is_active)                                    AS active_rows,
            count(*)                                                             AS total_rows,
            coalesce(sum(amount) FILTER (WHERE is_active), 0)                    AS amount_total,
            coalesce(sum(amount) FILTER (WHERE is_active AND type = 'IN'), 0)    AS amount_in,
            coalesce(sum(amount) FILTER (WHERE is_active AND type = 'OUT'), 0)   AS amount_out
        FROM movements_current
    """).fetchone()
    return {"active_rows": row[0], "total_rows": row[1], "amount_total": row[2],
            "amount_in": row[3], "amount_out": row[4]}


def apply_changes(db: Database, cfg: Config, *, run_id: str,
                  snapshot_date: date) -> dict:
    """Aplica tmp_changes. DEBE llamarse dentro de db.transaction().

    No abre transaccion propia: el llamador controla el limite atomico para que
    la actualizacion de pipeline_runs entre en el mismo COMMIT.
    """
    sql = db.sql_file("sql/persistence.sql")
    params = {"run_id": run_id, "snapshot_date": snapshot_date.isoformat()}

    statements = split_statements(sql)
    for position, statement in enumerate(statements, start=1):
        used = {k: v for k, v in params.items() if f"${k}" in statement}
        step = (PERSISTENCE_STEPS[position - 1]
                if position <= len(PERSISTENCE_STEPS)
                else "sentencia sin etiqueta")
        log.info("Persistencia %s/%s: %s", position, len(statements), step)
        try:
            db.con.execute(statement, used) if used else db.con.execute(statement)
        except Exception:
            # El pipeline conserva el tipo y el mensaje originales de DuckDB,
            # pero deja claro que operacion atomica los produjo. Esto es
            # imprescindible para diagnosticar fallos que solo aparecen con
            # millones de filas y que terminan correctamente en ROLLBACK.
            log.error("Fallo en persistencia %s/%s: %s",
                      position, len(statements), step)
            raise

    prune_history(db, cfg, snapshot_date=snapshot_date)

    summary = current_state_summary(db)
    log.info("Estado vigente tras la carga: %s activos / %s totales | monto vigente %.2f",
             f"{summary['active_rows']:,}", f"{summary['total_rows']:,}",
             float(summary["amount_total"]))
    return summary


def prune_history(db: Database, cfg: Config, *, snapshot_date: date) -> int:
    """Elimina versiones historicas cerradas mas antiguas que la retencion.

    Solo borra filas con `is_current = false`, asi que el estado vigente sigue
    siendo reconstruible al 100%. Lo que se pierde es poder responder "como
    estaba esto hace mas de N dias": por eso el defecto es null (sin purga), la
    retencion es una decision de negocio.

    Se ejecuta dentro de la transaccion abierta por el llamador.
    """
    days = cfg.get("persistence.history_retention_days")
    if not days:
        return 0

    deleted = db.execute(
        "DELETE FROM movements_history "
        "WHERE NOT is_current AND valid_to IS NOT NULL "
        "  AND valid_to < CAST(? AS DATE) - INTERVAL (?) DAY "
        "RETURNING 1", [snapshot_date.isoformat(), int(days)]).fetchall()

    n = len(deleted)
    if n:
        log.info("Retencion de historico: %s versiones cerradas anteriores a "
                 "%s dias eliminadas", f"{n:,}", days)
    return n


# --------------------------------------------------------- invariantes duras
INVARIANTS = {
    "unique_current_row_per_key": (
        "SELECT count(*) FROM (SELECT movement_key FROM movements_current "
        "GROUP BY 1 HAVING count(*) > 1)",
        "movements_current tiene mas de una fila para la misma movement_key"),
    "single_current_version_per_key": (
        "SELECT count(*) FROM (SELECT movement_key FROM movements_history "
        "WHERE is_current GROUP BY 1 HAVING count(*) > 1)",
        "movements_history tiene mas de una version marcada como vigente"),
    "history_covers_current": (
        "SELECT count(*) FROM movements_current c "
        "WHERE NOT EXISTS (SELECT 1 FROM movements_history h "
        "                  WHERE h.movement_key = c.movement_key)",
        "hay movimientos vigentes sin ninguna version en el historico"),
    # Las versiones conservadas deben ser consecutivas y terminar en max(version).
    # Con retencion activa se purgan las mas ANTIGUAS, por lo que la secuencia
    # puede empezar en un numero > 1; lo que no puede haber es un hueco intermedio.
    "no_version_gaps": (
        "SELECT count(*) FROM ("
        "  SELECT movement_key, max(version) - min(version) + 1 AS esperado, "
        "         count(*) AS n "
        "  FROM movements_history GROUP BY 1 HAVING esperado <> n)",
        "el historico tiene versiones no consecutivas"),
    "active_current_matches_open_version": (
        "SELECT count(*) FROM movements_current c "
        "JOIN movements_history h ON h.movement_key = c.movement_key AND h.is_current "
        "WHERE c.is_active AND (h.valid_to IS NOT NULL OR h.is_deleted)",
        "un movimiento activo apunta a una version historica ya cerrada"),
    "changes_have_events": (
        "SELECT count(*) FROM movement_changes mc "
        "WHERE mc.change_type = 'UPDATED' AND NOT EXISTS ("
        "  SELECT 1 FROM movement_change_fields f WHERE f.change_id = mc.change_id)",
        "hay correcciones registradas sin detalle de columnas modificadas"),
}


def check_invariants(db: Database) -> list[str]:
    """Verifica las invariantes estructurales. Devuelve la lista de violaciones."""
    violations = []
    for name, (sql, message) in INVARIANTS.items():
        count = db.scalar(sql) or 0
        if count:
            violations.append(f"{name}: {message} ({count} casos)")
    return violations
