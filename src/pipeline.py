"""Orquestador del pipeline.

Uso tipico:

    python -m src.pipeline --input-directory data/raw
    python -m src.pipeline --input data/raw/movimientos_2026-08-05.parquet \
                           --snapshot-date 2026-08-05

Codigos de salida:
    0  todo correcto (incluye "no habia nada nuevo que procesar")
    1  error critico de archivo, esquema o configuracion
    2  reconciliacion o invariante estructural fallida (los cambios se revirtieron)
    3  guarda de seguridad superada (los cambios NO se aplicaron)
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from src import analytics, reporting
from src.change_detection import detect_changes
from src.config import Config, load_config
from src.database import Database
from src.ingestion import CriticalIngestionError, FileMetadata, discover_files
from src.logging_config import get_logger, set_run_id, setup_logging
from src.normalization import build_staging
from src.observability import GuardBreach, evaluate_guards, raise_alert
from src.persistence import (apply_changes, check_invariants,
                             current_state_summary)
from src.reconciliation import ReconciliationFailed, run_reconciliation, write_reports

log = get_logger("pipeline")

PIPELINE_VERSION = "1.0.0"

EXIT_OK, EXIT_CRITICAL, EXIT_RECONCILIATION, EXIT_GUARD = 0, 1, 2, 3


class StructuralViolation(Exception):
    code = "STRUCTURAL_VIOLATION"


@dataclass
class SnapshotResult:
    run_id: str
    source_file: str
    snapshot_date: date
    status: str
    counts: dict
    exit_code: int = EXIT_OK


def _post_commit_warning(db: Database, run_id: str, code: str, message: str) -> None:
    """Registra un fallo operativo sin alterar una carga ya confirmada."""
    try:
        raise_alert(db, run_id, code, "WARNING", message)
    except Exception as alert_exc:  # noqa: BLE001 - no reabre la frontera de commit
        log.error("%s. Tampoco se pudo persistir la alerta %s: %s",
                  message, code, alert_exc)


# --------------------------------------------------------------- idempotencia
def _make_run_id(db: Database, meta: FileMetadata) -> str:
    """Identificador legible y determinista: <corte>__<hash corto>[__n]."""
    base = f"{meta.snapshot_date:%Y%m%d}__{meta.sha256[:12]}"
    existing = db.scalar(
        "SELECT count(*) FROM pipeline_runs "
        "WHERE run_id = ? OR starts_with(run_id, ?)",
        [base, base + "__"]) or 0
    return base if existing == 0 else f"{base}__{existing + 1}"


def _idempotency_decision(db: Database, cfg: Config, meta: FileMetadata,
                          force: bool) -> tuple[str, str, str | None]:
    """Devuelve (accion, motivo, run_id_previo). accion in {process, skip, reject}.

    La identidad de un corte es (contenido, fecha del corte), no el nombre del
    archivo ni su ruta. Reenviar el mismo contenido para el mismo corte es una
    repeticion y no debe aplicarse dos veces; el mismo contenido para un corte
    posterior es una reversion legitima del origen y si debe aplicarse.

    run_id_previo solo se devuelve para 'skip': quien omite debe conservar el
    run_id de la carga original en file_registry, no pisarlo con NULL.
    """
    prior = db.execute(
        "SELECT status, run_id FROM file_registry "
        "WHERE source_file_hash = ? AND snapshot_date = ?",
        [meta.sha256, meta.snapshot_date]).fetchone()

    if prior and prior[0] == "PROCESSED":
        if force or cfg.get("ingestion.known_hash_policy") == "force":
            return ("process",
                    f"contenido ya procesado en {prior[1]}, se reprocesa por --force", None)
        return ("skip", (f"contenido identico ya procesado con exito en la ejecucion "
                         f"{prior[1]}: no se vuelve a aplicar"), prior[1])

    # Mismo nombre de archivo pero contenido distinto.
    same_name = db.execute(
        "SELECT source_file_hash, run_id FROM file_registry "
        "WHERE source_file = ? AND source_file_hash <> ? AND status = 'PROCESSED'",
        [meta.name, meta.sha256]).fetchone()
    if same_name:
        policy = cfg.get("ingestion.same_name_new_hash_policy", "reprocess")
        motivo = (f"el archivo {meta.name} ya se proceso con otro contenido "
                  f"(hash {same_name[0][:12]}...); politica={policy}")
        if policy == "reject":
            return "reject", motivo, None
        return "process", motivo, None

    # Corte anterior al ultimo aplicado.
    if not cfg.get("ingestion.allow_out_of_order", False):
        last = db.scalar(
            "SELECT max(snapshot_date) FROM pipeline_runs WHERE status = 'SUCCESS'")
        if last and meta.snapshot_date and meta.snapshot_date < last:
            return ("reject", (f"el corte {meta.snapshot_date} es anterior al ultimo "
                               f"aplicado ({last}) y allow_out_of_order es false"), None)

    return "process", "corte nuevo", None


# ------------------------------------------------------------------ una carga
def process_snapshot(db: Database, cfg: Config, meta: FileMetadata,
                     force: bool = False) -> SnapshotResult:
    action, reason, prior_run_id = _idempotency_decision(db, cfg, meta, force)

    if action == "skip":
        log.info("OMITIDO %s: %s", meta.name, reason)
        # INSERT OR REPLACE reescribe la fila entera: prior_run_id preserva el
        # run_id de la carga original en lugar de dejarlo en NULL.
        db.execute(
            "INSERT OR REPLACE INTO file_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [meta.sha256, meta.name, str(meta.path), meta.size_bytes, meta.mtime,
             meta.snapshot_date, meta.row_count, meta.column_names,
             meta.schema_fingerprint, datetime.utcnow(), datetime.utcnow(),
             "PROCESSED", prior_run_id, None, reason])
        return SnapshotResult(prior_run_id or "-", meta.name, meta.snapshot_date,
                              "SKIPPED", {})

    if action == "reject":
        log.error("RECHAZADO %s: %s", meta.name, reason)
        raise CriticalIngestionError("FILE_OUT_OF_ORDER", reason)

    run_id = _make_run_id(db, meta)
    set_run_id(run_id)
    started = datetime.utcnow()
    t0 = time.perf_counter()
    log.info("Inicio de la carga de %s (%s filas, corte %s). Motivo: %s",
             meta.name, f"{meta.row_count:,}", meta.snapshot_date, reason)

    # La ejecucion se registra ANTES de tocar nada y fuera de la transaccion de
    # negocio: si el bloque atomico revierte, la traza del fallo sobrevive.
    db.execute(
        "INSERT INTO pipeline_runs (run_id, started_at, status, snapshot_date, "
        "input_file, input_hash, rows_read, pipeline_version, contract_version, "
        "contract_hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [run_id, started, "RUNNING", meta.snapshot_date, meta.name, meta.sha256,
         meta.row_count, PIPELINE_VERSION, cfg.contract_version, cfg.contract_hash])
    db.execute(
        "INSERT OR REPLACE INTO file_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [meta.sha256, meta.name, str(meta.path), meta.size_bytes, meta.mtime,
         meta.snapshot_date, meta.row_count, meta.column_names,
         meta.schema_fingerprint, datetime.utcnow(), None, "PROCESSING", run_id, None, None])

    def _fail(code: str, message: str, exit_code: int) -> SnapshotResult:
        db.execute(
            "UPDATE pipeline_runs SET status = ?, finished_at = ?, duration_seconds = ?, "
            "error_code = ?, error_message = ? WHERE run_id = ?",
            ["FAILED_GUARD" if exit_code == EXIT_GUARD else "FAILED", datetime.utcnow(),
             time.perf_counter() - t0, code, message[:2000], run_id])
        db.execute("UPDATE file_registry SET status = 'FAILED', error_code = ?, "
                   "error_message = ? WHERE source_file_hash = ? AND snapshot_date = ?",
                   [code, message[:2000], meta.sha256, meta.snapshot_date])
        log.error("Carga fallida (%s): %s", code, message)
        return SnapshotResult(run_id, meta.name, meta.snapshot_date, "FAILED", {}, exit_code)

    try:
        state_before = current_state_summary(db)

        # --- SILVER: tipado, normalizacion, validacion, cuarentena -------------
        stg = build_staging(
            db, cfg, path=str(meta.path), run_id=run_id, source_file=meta.name,
            source_file_hash=meta.sha256, snapshot_date=meta.snapshot_date)
        stg["rows_read"] = meta.row_count

        rejection_rate = stg["rows_rejected"] / meta.row_count if meta.row_count else 0.0
        thresholds = cfg.get("quality.max_rejection_rate", {})
        # Igual que las guardas: un porcentaje sobre un punado de filas no informa.
        rate_is_meaningful = meta.row_count >= int(
            cfg.get("guards.min_rows_for_ratio_guards", 100))
        if rate_is_meaningful and rejection_rate >= float(thresholds.get("fail", 1)):
            raise_alert(db, run_id, "REJECTION_RATE_EXCEEDED", "CRITICAL",
                        f"{rejection_rate:.2%} de filas rechazadas", rejection_rate,
                        float(thresholds["fail"]))
            return _fail("REJECTION_RATE_EXCEEDED",
                         f"El {rejection_rate:.2%} de las filas fue rechazado; "
                         f"el corte no se considera confiable.", EXIT_CRITICAL)
        if rate_is_meaningful and rejection_rate >= float(thresholds.get("warn", 1)):
            raise_alert(db, run_id, "HIGH_REJECTION_RATE", "WARNING",
                        f"{rejection_rate:.2%} de filas rechazadas", rejection_rate,
                        float(thresholds["warn"]))

        # --- Deteccion de cambios (aun sin escribir en GOLD) -------------------
        counts = detect_changes(db, cfg, run_id=run_id, snapshot_date=meta.snapshot_date)

        # --- Guardas de seguridad ---------------------------------------------
        prev_rows = db.scalar(
            "SELECT rows_valid FROM pipeline_runs WHERE status = 'SUCCESS' "
            "ORDER BY snapshot_date DESC, started_at DESC LIMIT 1")
        amount_after_est = db.scalar(
            "SELECT coalesce(sum(amount), 0) FROM stg_movements WHERE run_id = ?", [run_id])
        guard = evaluate_guards(
            db, cfg, run_id=run_id, snapshot_date=meta.snapshot_date, counts=counts,
            rows_valid=int(stg["rows_valid"]), previous_active=state_before["active_rows"],
            previous_rows=int(prev_rows) if prev_rows else None,
            amount_before=state_before["amount_total"], amount_after=amount_after_est)

        if not guard.passed:
            return _fail("GUARD_BREACH",
                         "Guardas de seguridad superadas; NO se aplicaron los cambios. "
                         "Revise el corte o ajuste los umbrales en config/pipeline.yml. "
                         "Detalle: " + "; ".join(guard.breaches), EXIT_GUARD)

        # --- GOLD: bloque atomico ---------------------------------------------
        with db.transaction():
            state_after = apply_changes(db, cfg, run_id=run_id,
                                        snapshot_date=meta.snapshot_date)

            violations = check_invariants(db)
            if violations:
                raise StructuralViolation("; ".join(violations))

            checks = run_reconciliation(
                db, cfg, run_id=run_id, snapshot_date=meta.snapshot_date,
                rows_read=meta.row_count, active_before=state_before["active_rows"],
                amount_before=state_before["amount_total"])
            if any(not c["passed"] for c in checks):
                failed = [c["check_name"] for c in checks if not c["passed"]]
                raise ReconciliationFailed("; ".join(failed))

            db.execute(
                "UPDATE pipeline_runs SET status = ?, finished_at = ?, duration_seconds = ?, "
                "rows_valid = ?, rows_rejected = ?, rows_exact_dupes = ?, rows_new = ?, "
                "rows_updated = ?, rows_deleted = ?, rows_unchanged = ?, rows_reactivated = ?, "
                "rows_current_before = ?, rows_current_after = ?, "
                "amount_in = ?, amount_out = ?, amount_current = ? "
                "WHERE run_id = ?",
                ["SUCCESS", datetime.utcnow(), time.perf_counter() - t0,
                 int(stg["rows_valid"]), int(stg["rows_rejected"]),
                 int(stg["rows_exact_dupes"]), counts["NEW"], counts["UPDATED"],
                 counts["DELETED"], counts["UNCHANGED"], counts["REACTIVATED"],
                 int(state_before["active_rows"]),
                 state_after["active_rows"], state_after["amount_in"],
                 state_after["amount_out"], state_after["amount_total"], run_id])

            # Confirma GOLD, run y archivo juntos para que un reintento no
            # vuelva a aplicar datos ya persistidos.
            db.execute("UPDATE file_registry SET status = 'PROCESSED', processed_at = ? "
                       "WHERE source_file_hash = ? AND snapshot_date = ?",
                       [datetime.utcnow(), meta.sha256, meta.snapshot_date])

        # Silver es transitorio. Los fallos posteriores al commit se alertan sin
        # cambiar el estado de una carga ya confirmada.
        try:
            db.execute("DELETE FROM stg_movements WHERE run_id = ?", [run_id])
        except Exception as exc:  # noqa: BLE001 - fallo operativo post-commit
            message = f"No se pudo limpiar staging tras el COMMIT: {type(exc).__name__}: {exc}"
            _post_commit_warning(db, run_id, "STAGING_CLEANUP_FAILED", message)

        try:
            write_reports(db, cfg, run_id=run_id, snapshot_date=meta.snapshot_date,
                          counts=counts, staging_metrics=stg,
                          state_before=state_before, state_after=state_after)
        except Exception as exc:  # noqa: BLE001 - fallo operativo post-commit
            message = f"Datos confirmados; fallo al publicar reportes: {type(exc).__name__}: {exc}"
            _post_commit_warning(db, run_id, "REPORTING_FAILED", message)

        log.info("Carga completada en %.2fs", time.perf_counter() - t0)
        return SnapshotResult(run_id, meta.name, meta.snapshot_date, "SUCCESS", counts)

    except (ReconciliationFailed, StructuralViolation) as exc:
        return _fail(type(exc).code, str(exc), EXIT_RECONCILIATION)
    except GuardBreach as exc:
        return _fail("GUARD_BREACH", str(exc), EXIT_GUARD)
    except CriticalIngestionError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.debug(traceback.format_exc())
        return _fail("UNEXPECTED_ERROR", f"{type(exc).__name__}: {exc}", EXIT_CRITICAL)


# ------------------------------------------------------------------------ CLI
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.pipeline",
        description="Pipeline de movimientos financieros: ingesta cortes diarios en "
                    "parquet y mantiene una base consultable, historificada y auditable.")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--input", type=Path, help="Ruta de un unico archivo parquet")
    src.add_argument("--input-directory", type=Path,
                     help="Directorio con los cortes pendientes (por defecto data/raw)")
    p.add_argument("--snapshot-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                   help="Fecha del corte (solo con --input). Es la fuente mas confiable.")
    p.add_argument("--force", action="store_true",
                   help="Reprocesa aunque el contenido ya se haya aplicado")
    p.add_argument("--skip-analytics", action="store_true",
                   help="No genera los reportes analiticos al final")
    p.add_argument("--config", type=str, default=None, help="Ruta alternativa de pipeline.yml")
    p.add_argument("--log-level", type=str, default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    setup_logging(args.log_level or cfg.get("logging.level", "INFO"),
                  cfg.get("logging.format", "text"),
                  cfg.paths.reports / "pipeline.log")
    cfg.paths.ensure()

    log.info("Pipeline v%s | raiz del proyecto: %s", PIPELINE_VERSION, cfg.root)

    db: Database | None = None
    worst_exit = EXIT_OK

    try:
        db = Database(cfg)
        db.init_schema()
        metas = discover_files(cfg, input_path=args.input,
                               input_dir=args.input_directory,
                               cli_date=args.snapshot_date)
        log.info("Se procesaran %d archivo(s) en este orden: %s",
                 len(metas), ", ".join(m.name for m in metas))

        results: list[SnapshotResult] = []
        for meta in metas:
            result = process_snapshot(db, cfg, meta, force=args.force)
            results.append(result)
            set_run_id("-")
            if result.exit_code != EXIT_OK:
                worst_exit = max(worst_exit, result.exit_code)
                log.error("Se detiene el procesamiento en %s para no aplicar cortes "
                          "posteriores sobre un estado no confiable.", meta.name)
                break

        if worst_exit == EXIT_OK and not args.skip_analytics:
            analytics.build_analytics(db, cfg)
            reporting.write_all(db, cfg)

        _print_summary(results)

    except CriticalIngestionError as exc:
        log.error("ERROR CRITICO [%s] %s", exc.code, exc.message)
        worst_exit = EXIT_CRITICAL
    except Exception as exc:  # noqa: BLE001
        log.error("ERROR NO CONTROLADO: %s", exc)
        log.debug(traceback.format_exc())
        worst_exit = EXIT_CRITICAL
    finally:
        if db is not None:
            db.close()

    log.info("Pipeline finalizado con codigo de salida %d", worst_exit)
    return worst_exit


def _print_summary(results: list[SnapshotResult]) -> None:
    if not results:
        return
    log.info("=" * 78)
    log.info("RESUMEN DE LA EJECUCION")
    for r in results:
        if r.status == "SUCCESS":
            log.info("  %-32s %s  NEW=%s UPDATED=%s DELETED=%s UNCHANGED=%s",
                     r.source_file, r.status, f"{r.counts['NEW']:,}",
                     f"{r.counts['UPDATED']:,}", f"{r.counts['DELETED']:,}",
                     f"{r.counts['UNCHANGED']:,}")
        else:
            log.info("  %-32s %s", r.source_file, r.status)
    log.info("=" * 78)


if __name__ == "__main__":
    sys.exit(main())
