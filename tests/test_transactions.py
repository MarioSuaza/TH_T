"""Atomicidad: un fallo intermedio no puede dejar la base a medias."""

from __future__ import annotations

import pytest

from src.persistence import check_invariants
from tests.conftest import row


def _state(db) -> dict:
    return {
        "current": db.scalar("SELECT count(*) FROM movements_current"),
        "active": db.scalar("SELECT count(*) FROM movements_current WHERE is_active"),
        "history": db.scalar("SELECT count(*) FROM movements_history"),
        "changes": db.scalar("SELECT count(*) FROM movement_changes"),
        "fields": db.scalar("SELECT count(*) FROM movement_change_fields"),
        "amount": db.scalar("SELECT coalesce(sum(amount), 0) FROM movements_current"),
        "recon": db.scalar("SELECT count(*) FROM reconciliation_results"),
    }


def test_una_excepcion_dentro_de_la_transaccion_revierte_todo(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001", amount=100.0)], name="t.parquet"),
         "2024-09-15")
    before = _state(db)

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with db.transaction():
            # Escrituras validas en las tres tablas de GOLD...
            db.execute("UPDATE movements_current SET amount = 999999")
            db.execute("UPDATE movements_history SET amount = 999999")
            db.execute("INSERT INTO reconciliation_results "
                       "(run_id, check_group, check_name, passed) "
                       "VALUES ('x', 'g', 'c', TRUE)")
            # ...y un fallo justo antes del COMMIT.
            raise Boom("fallo simulado a mitad de la escritura")

    assert _state(db) == before, "el ROLLBACK debe dejar la base exactamente igual"


def test_un_fallo_al_escribir_el_historico_no_deja_el_estado_actualizado(
        db, cfg, make_parquet, load, monkeypatch):
    """Se rompe apply_changes a mitad y se verifica que nada persiste."""
    from src import pipeline as pipeline_mod

    load(make_parquet([row(id_cliente="CLI000001", amount=100.0)], name="t.parquet"),
         "2024-09-15")
    before = _state(db)

    def exploding_apply(db_, cfg_, *, run_id, snapshot_date):
        # Escribe una parte y falla: es el peor caso posible.
        db_.execute("UPDATE movements_current SET amount = 1")
        raise RuntimeError("fallo durante la escritura del historico")

    monkeypatch.setattr(pipeline_mod, "apply_changes", exploding_apply)
    result = load(make_parquet([row(id_cliente="CLI000001", amount=500.0)],
                               name="t1.parquet"), "2024-09-16")

    assert result.status == "FAILED"
    assert _state(db) == before
    assert check_invariants(db) == []


def test_la_ejecucion_fallida_queda_marcada_como_fallida(db, cfg, make_parquet, load,
                                                         monkeypatch):
    from src import pipeline as pipeline_mod

    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")

    def exploding_apply(db_, cfg_, *, run_id, snapshot_date):
        raise RuntimeError("fallo simulado")

    monkeypatch.setattr(pipeline_mod, "apply_changes", exploding_apply)
    result = load(make_parquet([row(amount=500.0)], name="t1.parquet"), "2024-09-16")

    run = db.execute("SELECT status, error_code, error_message FROM pipeline_runs "
                     "WHERE run_id = ?", [result.run_id]).fetchone()
    assert run[0] == "FAILED"
    assert run[1] == "UNEXPECTED_ERROR"
    assert "fallo simulado" in run[2]
    assert db.scalar("SELECT count(*) FROM pipeline_runs WHERE status = 'SUCCESS'") == 1


def test_una_reconciliacion_fallida_revierte_la_carga(db, cfg, make_parquet, load,
                                                      monkeypatch):
    from src import pipeline as pipeline_mod

    load(make_parquet([row(id_cliente="CLI000001", amount=100.0)], name="t.parquet"),
         "2024-09-15")
    before = _state(db)

    def failing_reconciliation(*args, **kwargs):
        return [{"check_name": "control simulado", "passed": False}]

    monkeypatch.setattr(pipeline_mod, "run_reconciliation", failing_reconciliation)
    result = load(make_parquet([row(id_cliente="CLI000001", amount=500.0)],
                               name="t1.parquet"), "2024-09-16")

    assert result.status == "FAILED"
    assert result.exit_code == pipeline_mod.EXIT_RECONCILIATION
    assert _state(db) == before
    assert db.scalar("SELECT error_code FROM pipeline_runs WHERE run_id = ?",
                     [result.run_id]) == "RECONCILIATION_FAILED"


def test_una_violacion_de_invariante_revierte_la_carga(db, cfg, make_parquet, load,
                                                       monkeypatch):
    from src import pipeline as pipeline_mod

    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    before = _state(db)

    monkeypatch.setattr(pipeline_mod, "check_invariants",
                        lambda _db: ["invariante simulada: dos filas vigentes"])
    result = load(make_parquet([row(amount=500.0)], name="t1.parquet"), "2024-09-16")

    assert result.exit_code == pipeline_mod.EXIT_RECONCILIATION
    assert _state(db) == before


def test_tras_un_fallo_se_puede_reintentar_con_exito(db, cfg, make_parquet, load,
                                                     monkeypatch):
    from src import pipeline as pipeline_mod

    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    t1 = make_parquet([row(amount=500.0)], name="t1.parquet")

    real_apply = pipeline_mod.apply_changes
    monkeypatch.setattr(pipeline_mod, "apply_changes",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fallo")))
    assert load(t1, "2024-09-16").status == "FAILED"

    monkeypatch.setattr(pipeline_mod, "apply_changes", real_apply)
    result = load(t1, "2024-09-16")
    assert result.status == "SUCCESS"
    assert result.counts["UPDATED"] == 1
    assert float(db.scalar("SELECT amount FROM movements_current")) == 500.0
    assert check_invariants(db) == []


def test_un_fallo_de_reporte_post_commit_no_marca_la_carga_como_fallida(
        db, cfg, make_parquet, load, monkeypatch):
    from src import pipeline as pipeline_mod

    monkeypatch.setattr(
        pipeline_mod, "write_reports",
        lambda *a, **k: (_ for _ in ()).throw(OSError("disco de reportes no disponible")))

    result = load(make_parquet([row(amount=321.0)], name="t.parquet"), "2024-09-15")

    assert result.status == "SUCCESS"
    assert db.scalar("SELECT status FROM pipeline_runs WHERE run_id = ?", [result.run_id]) == "SUCCESS"
    assert db.scalar("SELECT status FROM file_registry WHERE run_id = ?", [result.run_id]) == "PROCESSED"
    assert float(db.scalar("SELECT amount FROM movements_current WHERE is_active")) == 321.0
    assert db.scalar("SELECT count(*) FROM run_alerts WHERE run_id = ? "
                     "AND alert_code = 'REPORTING_FAILED'", [result.run_id]) == 1
