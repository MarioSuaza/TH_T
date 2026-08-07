"""Reconciliaciones de volumen y monetarias, y sus reportes por ejecucion."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from src.config import Config
from src.database import Database
from src.logging_config import get_logger
from src.normalization import split_statements

log = get_logger("reconciliation")


class ReconciliationFailed(Exception):
    code = "RECONCILIATION_FAILED"


def run_reconciliation(db: Database, cfg: Config, *, run_id: str, snapshot_date: date,
                       rows_read: int, active_before: int, amount_before) -> list[dict]:
    """Ejecuta los controles y los persiste. DEBE ir dentro de la transaccion."""
    sql = db.sql_file("sql/reconciliation.sql")
    params = {
        "run_id": run_id,
        "snapshot_date": snapshot_date.isoformat(),
        "rows_read": int(rows_read),
        "active_before": int(active_before),
        "amount_before": str(amount_before or 0),
    }
    for statement in split_statements(sql):
        used = {k: v for k, v in params.items() if f"${k}" in statement}
        db.con.execute(statement, used) if used else db.con.execute(statement)

    rows = db.execute(
        "SELECT check_group, check_name, left_label, left_value, right_label, "
        "right_value, difference, passed FROM reconciliation_results "
        "WHERE run_id = ? ORDER BY check_group, check_name", [run_id]).df()

    failed = rows[~rows["passed"]]
    if len(failed):
        for _, r in failed.iterrows():
            log.error("Reconciliacion FALLIDA [%s] %s: %s=%s vs %s=%s (dif %s)",
                      r.check_group, r.check_name, r.left_label, r.left_value,
                      r.right_label, r.right_value, r.difference)
    else:
        log.info("Reconciliacion: %s controles, todos cuadran", len(rows))

    return rows.to_dict("records")


# ------------------------------------------------------------------- agregados
AGGREGATE_SQL = """
SELECT
    '{dim}' AS dimension,
    CAST({col} AS VARCHAR) AS value,
    count(*) AS movements,
    coalesce(sum(amount), 0) AS amount_total,
    coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0) AS amount_in,
    coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0) AS amount_out
FROM movements_current
WHERE is_active
GROUP BY 2
ORDER BY 4 DESC
"""


def aggregates_by_dimension(db: Database) -> "object":
    """Sumas por type / fund / product / commercial_name sobre el estado vigente."""
    import pandas as pd

    frames = [db.df(AGGREGATE_SQL.format(dim=dim, col=col))
              for dim, col in (("type", "type"), ("fund", "fund"),
                               ("product", "product"),
                               ("commercial_name", "coalesce(commercial_name, '(sin nombre)')"))]
    return pd.concat(frames, ignore_index=True)


# -------------------------------------------------------------------- reportes
def write_reports(db: Database, cfg: Config, *, run_id: str, snapshot_date: date,
                  counts: dict, staging_metrics: dict, state_before: dict,
                  state_after: dict) -> list[Path]:
    """Escribe reports/reconciliation_<run_id>.csv y .md."""
    out_dir = cfg.paths.reports
    out_dir.mkdir(parents=True, exist_ok=True)

    checks = db.df(
        "SELECT check_group, check_name, left_label, left_value, right_label, "
        "right_value, difference, passed FROM reconciliation_results "
        "WHERE run_id = ? ORDER BY check_group, check_name", [run_id])
    csv_path = out_dir / f"reconciliation_{run_id}.csv"
    checks.to_csv(csv_path, index=False)

    aggs = aggregates_by_dimension(db)
    aggs_path = out_dir / f"reconciliation_aggregates_{run_id}.csv"
    aggs.to_csv(aggs_path, index=False)

    rejected = db.df(
        "SELECT error_code, error_severity, count(*) AS rows "
        "FROM rejected_records WHERE run_id = ? GROUP BY 1, 2 ORDER BY 3 DESC", [run_id])

    md_path = out_dir / f"reconciliation_{run_id}.md"
    all_pass = bool(checks["passed"].all()) if len(checks) else True

    lines = [
        f"# Reconciliacion - ejecucion `{run_id}`",
        "",
        f"- **Corte:** {snapshot_date}",
        f"- **Resultado global:** {'TODOS LOS CONTROLES CUADRAN' if all_pass else 'HAY CONTROLES FALLIDOS'}",
        "",
        "## Volumen",
        "",
        "| Metrica | Valor |",
        "| --- | ---: |",
        f"| Filas leidas del archivo | {int(staging_metrics['rows_read']):,} |",
        f"| Filas validas (distintas) | {int(staging_metrics['rows_valid']):,} |",
        f"| Duplicados exactos colapsados | {int(staging_metrics['rows_exact_dupes']):,} |",
        f"| Filas rechazadas (cuarentena) | {int(staging_metrics['rows_rejected']):,} |",
        f"| Claves de negocio ambiguas | {int(staging_metrics['rows_key_ambiguous']):,} |",
        "",
        "## Clasificacion de cambios",
        "",
        "| Tipo | Movimientos |",
        "| --- | ---: |",
        f"| NEW | {counts.get('NEW', 0):,} |",
        f"| UPDATED | {counts.get('UPDATED', 0):,} |",
        f"| DELETED | {counts.get('DELETED', 0):,} |",
        f"| UNCHANGED | {counts.get('UNCHANGED', 0):,} |",
        f"| REACTIVATED | {counts.get('REACTIVATED', 0):,} |",
        f"| Ya dados de baja (sin cambio) | {counts.get('STILL_DELETED', 0):,} |",
        "",
        "## Estado vigente",
        "",
        "| Metrica | Antes | Despues |",
        "| --- | ---: | ---: |",
        f"| Movimientos activos | {state_before['active_rows']:,} | {state_after['active_rows']:,} |",
        f"| Monto vigente | {float(state_before['amount_total']):,.2f} | {float(state_after['amount_total']):,.2f} |",
        f"| Monto IN | {float(state_before['amount_in']):,.2f} | {float(state_after['amount_in']):,.2f} |",
        f"| Monto OUT | {float(state_before['amount_out']):,.2f} | {float(state_after['amount_out']):,.2f} |",
        "",
        "## Controles",
        "",
        "| Grupo | Control | Izquierda | Derecha | Diferencia | Resultado |",
        "| --- | --- | ---: | ---: | ---: | :---: |",
    ]
    for _, r in checks.iterrows():
        lines.append(
            f"| {r.check_group} | {r.check_name} | {r.left_label}={r.left_value:,.2f} | "
            f"{r.right_label}={r.right_value:,.2f} | {r.difference:,.2f} | "
            f"{'OK' if r.passed else 'FALLA'} |")

    if len(rejected):
        lines += ["", "## Registros en cuarentena", "",
                  "| Codigo | Severidad | Filas |", "| --- | --- | ---: |"]
        for _, r in rejected.iterrows():
            lines.append(f"| {r.error_code} | {r.error_severity} | {int(r['rows']):,} |")

    lines += ["", "## Sumas por dimension (estado vigente)", "",
              "| Dimension | Valor | Movimientos | Monto total |",
              "| --- | --- | ---: | ---: |"]
    for _, r in aggs.iterrows():
        lines.append(f"| {r.dimension} | {r.value} | {int(r.movements):,} | "
                     f"{float(r.amount_total):,.2f} |")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Reportes de reconciliacion escritos en %s", out_dir)
    return [csv_path, aggs_path, md_path]
