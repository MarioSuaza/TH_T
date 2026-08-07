"""Capa analitica y deteccion de eventos extranos.

Dos principios:
1. Ningun indicador se inventa: todos se calculan sobre los datos persistidos y
   quedan reproducibles con la consulta que se documenta junto a la metrica.
2. Una anomalia NO es una afirmacion de fraude ni de error. Es una desviacion
   respecto a un criterio explicito y configurable que merece revision humana.
"""

from __future__ import annotations

from src.config import Config
from src.database import Database
from src.logging_config import get_logger
from src.normalization import split_statements

log = get_logger("analytics")


def create_views(db: Database, cfg: Config) -> None:
    for statement in split_statements(db.sql_file("sql/analytics.sql")):
        db.con.execute(statement)
    log.info("Vistas analiticas creadas")


# ---------------------------------------------------------------- anomalias
def _insert_anomaly(db: Database, run_id, snapshot_date, code, category, severity,
                    entity_type, entity_id, metric, observed, expected, threshold,
                    description) -> None:
    db.execute(
        "INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity, "
        "entity_type, entity_id, metric_name, observed, expected, threshold, description) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [run_id, snapshot_date, code, category, severity, entity_type, entity_id,
         metric, observed, expected, threshold, description])


def detect_anomalies(db: Database, cfg: Config) -> int:
    """Recalcula la tabla de anomalias desde cero sobre el estado actual."""
    db.execute("DELETE FROM anomalies")

    last_run = db.execute(
        "SELECT run_id, snapshot_date FROM pipeline_runs WHERE status = 'SUCCESS' "
        "ORDER BY snapshot_date DESC, started_at DESC LIMIT 1").fetchone()
    run_id, snap = (last_run or (None, None))

    # ---------------------------------------------------------------- 1. IQR
    # Regla intercuartilica de Tukey: transparente, sin supuesto de normalidad y
    # explicable ante un auditor. Se aplica por (fund, type) porque las escalas
    # difieren entre fondos.
    mult = float(cfg.get("analytics.iqr_multiplier", 3.0))
    if cfg.get("analytics.outlier_method", "iqr") == "iqr":
        db.execute(f"""
            INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity,
                                   entity_type, entity_id, metric_name, observed, expected,
                                   threshold, description)
            WITH bounds AS (
                SELECT fund, type,
                       quantile_cont(amount, 0.25) AS q1,
                       quantile_cont(amount, 0.75) AS q3
                FROM v_movements_active GROUP BY 1, 2
            )
            SELECT ?, ?, 'AMOUNT_OUTLIER', 'OUTLIER', 'INFO', 'movement', m.movement_key,
                   'amount', CAST(m.amount AS DOUBLE),
                   CAST((b.q1 + b.q3) / 2 AS DOUBLE),
                   CAST({mult} AS DOUBLE),
                   'Monto fuera del rango intercuartilico x{mult} para el fondo '
                     || m.fund || ' / ' || m.type
                     || ' (limites ' || CAST(round(b.q1 - {mult} * (b.q3 - b.q1), 2) AS VARCHAR)
                     || ' .. ' || CAST(round(b.q3 + {mult} * (b.q3 - b.q1), 2) AS VARCHAR) || ')'
            FROM v_movements_active m
            JOIN bounds b ON b.fund = m.fund AND b.type = m.type
            WHERE (b.q3 - b.q1) > 0
              AND (m.amount < b.q1 - {mult} * (b.q3 - b.q1)
                OR m.amount > b.q3 + {mult} * (b.q3 - b.q1))
        """, [run_id, snap])

    # ------------------------------------------- 2. Valores categoricos nuevos
    if cfg.get("analytics.anomaly.new_category_alert", True):
        db.execute("""
            INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity,
                                   entity_type, entity_id, metric_name, observed, expected,
                                   threshold, description)
            SELECT run_id, snapshot_date, 'NEW_CATEGORY_VALUE', 'QUALITY', 'WARNING',
                   column_name, flag_code, 'occurrences', CAST(count(*) AS DOUBLE), 0, 0,
                   'Aparecen valores fuera del catalogo observado en ' || column_name
                     || ': ' || CAST(count(*) AS VARCHAR) || ' registros'
            FROM data_quality_flags
            WHERE flag_code IN ('UNKNOWN_FUND', 'UNKNOWN_PRODUCT')
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        """)

    # ------------------------------------ 3. Volumen diario atipico (z-score)
    z = float(cfg.get("analytics.anomaly.daily_volume_zscore_threshold", 3.0))
    db.execute(f"""
        INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity,
                               entity_type, entity_id, metric_name, observed, expected,
                               threshold, description)
        WITH s AS (SELECT avg(movements) AS mu, stddev_samp(movements) AS sd FROM v_daily_movements)
        SELECT ?, ?, 'DAILY_VOLUME_OUTLIER', 'REVIEW', 'INFO', 'movement_date',
               CAST(d.movement_date AS VARCHAR), 'movements',
               CAST(d.movements AS DOUBLE), s.mu, {z},
               'El dia ' || CAST(d.movement_date AS VARCHAR) || ' tiene ' ||
               CAST(d.movements AS VARCHAR) || ' movimientos frente a una media de ' ||
               CAST(round(s.mu, 1) AS VARCHAR) || ' (z=' ||
               CAST(round((d.movements - s.mu) / nullif(s.sd, 0), 2) AS VARCHAR) || ')'
        FROM v_daily_movements d, s
        WHERE s.sd > 0 AND abs((d.movements - s.mu) / s.sd) >= {z}
    """, [run_id, snap])

    # ------------------------------- 4. Concentracion de bajas / correcciones
    db.execute("""
        INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity,
                               entity_type, entity_id, metric_name, observed, expected,
                               threshold, description)
        SELECT run_id, snapshot_date, 'HIGH_DELETION_SHARE', 'CHANGE', 'WARNING',
               'snapshot', CAST(snapshot_date AS VARCHAR), 'deleted_share',
               rows_deleted * 1.0 / nullif(rows_current_before, 0), 0, 0.10,
               'El corte dio de baja ' || CAST(rows_deleted AS VARCHAR) ||
               ' movimientos, el ' ||
               CAST(round(100.0 * rows_deleted / nullif(rows_current_before, 0), 1) AS VARCHAR) ||
               '% de los ' || CAST(rows_current_before AS VARCHAR) ||
               ' vigentes antes del corte'
        FROM pipeline_runs
        WHERE status = 'SUCCESS'
          AND rows_deleted * 1.0 / nullif(rows_current_before, 0) >= 0.10
    """)

    # ----------------------------------- 5. Signo del monto frente al sentido
    # NO se impone ninguna regla: se REPORTA la relacion observada para que un
    # humano decida si es una convencion de signo o un error de origen.
    db.execute("""
        INSERT INTO anomalies (run_id, snapshot_date, anomaly_code, category, severity,
                               entity_type, entity_id, metric_name, observed, expected,
                               threshold, description)
        SELECT ?, ?, 'NEGATIVE_AMOUNT_CONCENTRATION', 'BUSINESS_RULE', 'WARNING',
               'type', type, 'negative_share',
               negative_amounts * 1.0 / nullif(movements, 0), NULL, NULL,
               'El ' || CAST(round(100.0 * negative_amounts / nullif(movements, 0), 2) AS VARCHAR) ||
               '% de los movimientos ' || type || ' tiene monto negativo (' ||
               CAST(negative_amounts AS VARCHAR) || ' de ' || CAST(movements AS VARCHAR) ||
               '). El enunciado no define el signo: se reporta, no se corrige.'
        FROM v_summary_by_type WHERE negative_amounts > 0
    """, [run_id, snap])

    total = db.scalar("SELECT count(*) FROM anomalies") or 0
    log.info("Deteccion de anomalias completada: %s registros", f"{total:,}")
    return total


def build_analytics(db: Database, cfg: Config) -> None:
    if not cfg.get("analytics.enabled", True):
        log.info("Analitica deshabilitada por configuracion")
        return
    create_views(db, cfg)
    detect_anomalies(db, cfg)
