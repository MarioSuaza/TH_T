"""Metricas, alertas y guardas de seguridad.

Las guardas viven aqui porque su unica salida es una alerta con un umbral: son
observabilidad con capacidad de veto. Un umbral superado NO es una regla de
negocio, es un control operativo configurable en config/pipeline.yml.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from src.config import Config
from src.database import Database
from src.logging_config import get_logger

log = get_logger("observability")


# ---------------------------------------------------------------------- alertas
def raise_alert(db: Database, run_id: str, code: str, severity: str, message: str,
                observed: float | None = None, threshold: float | None = None) -> None:
    db.execute(
        "INSERT INTO run_alerts (run_id, alert_code, severity, message, observed, threshold) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [run_id, code, severity, message,
         None if observed is None else float(observed),
         None if threshold is None else float(threshold)])
    logger = {"CRITICAL": log.error, "WARNING": log.warning}.get(severity, log.info)
    logger("ALERTA %s [%s] %s", code, severity, message)


# ----------------------------------------------------------------------- guardas
@dataclass
class GuardResult:
    passed: bool
    breaches: list[str]

    def __bool__(self) -> bool:
        return self.passed


class GuardBreach(Exception):
    """Un umbral de seguridad se supero y la politica configurada es detener."""

    code = "GUARD_BREACH"


def _check(db: Database, run_id: str, cfg_thresholds: dict, observed: float,
           code: str, label: str, breaches: list[str]) -> None:
    """Evalua un umbral de dos niveles (warn / fail)."""
    if observed is None:
        return
    warn = float(cfg_thresholds.get("warn", 1e9))
    fail = float(cfg_thresholds.get("fail", 1e9))
    if observed >= fail:
        raise_alert(db, run_id, code, "CRITICAL",
                    f"{label}: {observed:.2%} supera el umbral de fallo {fail:.2%}",
                    observed, fail)
        breaches.append(f"{code}={observed:.4f} (fail>={fail})")
    elif observed >= warn:
        raise_alert(db, run_id, code, "WARNING",
                    f"{label}: {observed:.2%} supera el umbral de aviso {warn:.2%}",
                    observed, warn)


def evaluate_guards(db: Database, cfg: Config, *, run_id: str, snapshot_date: date,
                    counts: dict, rows_valid: int,
                    previous_active: int, previous_rows: int | None,
                    amount_before, amount_after) -> GuardResult:
    """Controles contra cortes parciales, incompletos o corruptos.

    Se ejecuta ANTES de escribir nada. Si la politica es `fail`, la ejecucion se
    aborta sin aplicar las eliminaciones.
    """
    breaches: list[str] = []
    if not cfg.get("guards.enabled", True):
        return GuardResult(True, breaches)

    g = cfg.get("guards", {})

    # 1. Volumen minimo de filas validas.
    min_rows = int(g.get("min_valid_rows", 1))
    if rows_valid < min_rows:
        raise_alert(db, run_id, "MIN_VALID_ROWS", "CRITICAL",
                    f"Solo {rows_valid} filas validas, minimo esperado {min_rows}",
                    rows_valid, min_rows)
        breaches.append(f"MIN_VALID_ROWS={rows_valid}")

    # Los umbrales porcentuales solo son informativos por encima de cierto volumen:
    # con 3 movimientos vigentes, una baja es el 33% y ese numero no dice nada.
    min_rows = int(g.get("min_rows_for_ratio_guards", 100))
    scale_ok = max(previous_active, rows_valid) >= min_rows
    if not scale_ok:
        log.debug("Volumen por debajo de %s filas: no se aplican las guardas "
                  "porcentuales, solo los controles absolutos.", min_rows)
        return GuardResult(not breaches, breaches)

    # 2. Porcentaje de bajas sobre el estado vigente previo.
    if previous_active > 0:
        deleted_pct = counts.get("DELETED", 0) / previous_active
        _check(db, run_id, g.get("max_deleted_pct", {}), deleted_pct,
               "HIGH_DELETION_RATE",
               f"{counts.get('DELETED', 0):,} bajas sobre {previous_active:,} vigentes",
               breaches)

    # 3. Caida de volumen respecto al corte anterior.
    if previous_rows:
        drop = max(0.0, (previous_rows - rows_valid) / previous_rows)
        _check(db, run_id, g.get("max_volume_drop_pct", {}), drop,
               "VOLUME_DROP",
               f"El corte trae {rows_valid:,} filas validas frente a {previous_rows:,} del anterior",
               breaches)

    # 4. Variacion del monto total vigente.
    if amount_before:
        before, after = float(amount_before), float(amount_after or 0)
        variation = abs(after - before) / abs(before) if before else 0.0
        _check(db, run_id, g.get("max_amount_variation_pct", {}), variation,
               "AMOUNT_VARIATION",
               f"El monto vigente pasa de {before:,.2f} a {after:,.2f}",
               breaches)

    if breaches:
        policy = cfg.get("guards.on_breach", "fail")
        if policy == "warn_only":
            log.warning("Guardas superadas pero la politica es warn_only: se continua. %s", breaches)
            return GuardResult(True, breaches)
        return GuardResult(False, breaches)

    return GuardResult(True, breaches)
