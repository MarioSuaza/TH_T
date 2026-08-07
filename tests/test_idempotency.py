"""Idempotencia: reprocesar no duplica, no reescribe y queda registrado."""

from __future__ import annotations

import shutil

import pandas as pd

from tests.conftest import COLUMNS, row


def _snapshot_state(db) -> dict:
    return {
        "current": db.scalar("SELECT count(*) FROM movements_current"),
        "active": db.scalar("SELECT count(*) FROM movements_current WHERE is_active"),
        "history": db.scalar("SELECT count(*) FROM movements_history"),
        "changes": db.scalar("SELECT count(*) FROM movement_changes"),
        "fields": db.scalar("SELECT count(*) FROM movement_change_fields"),
        "rejected": db.scalar("SELECT count(*) FROM rejected_records"),
        "amount": db.scalar("SELECT coalesce(sum(amount), 0) FROM movements_current WHERE is_active"),
        "max_version": db.scalar("SELECT coalesce(max(version), 0) FROM movements_history"),
    }


def test_reprocesar_el_mismo_archivo_no_cambia_nada(db, cfg, make_parquet, load):
    path = make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 4)],
                        name="t.parquet")
    load(path, "2024-09-15")
    before = _snapshot_state(db)

    r2 = load(path, "2024-09-15")
    assert r2.status == "SKIPPED"
    assert _snapshot_state(db) == before


def test_reprocesar_no_duplica_metricas_ni_ejecuciones(db, cfg, make_parquet, load):
    path = make_parquet([row()], name="t.parquet")
    load(path, "2024-09-15")
    load(path, "2024-09-15")
    load(path, "2024-09-15")
    assert db.scalar("SELECT count(*) FROM pipeline_runs WHERE status = 'SUCCESS'") == 1


def test_el_archivo_omitido_queda_registrado(db, cfg, make_parquet, load):
    path = make_parquet([row()], name="t.parquet")
    load(path, "2024-09-15")
    load(path, "2024-09-15")
    r = db.execute("SELECT status, error_message FROM file_registry").fetchone()
    assert r[0] == "PROCESSED"
    assert "ya procesado" in (r[1] or "")


def test_mismo_contenido_en_otra_ruta_tambien_se_omite(db, cfg, make_parquet, load, tmp_path):
    original = make_parquet([row()], name="t.parquet")
    otra_ruta = tmp_path / "subcarpeta"
    otra_ruta.mkdir()
    copia = otra_ruta / "otro_nombre.parquet"
    shutil.copy(original, copia)

    load(original, "2024-09-15")
    before = _snapshot_state(db)
    r = load(copia, "2024-09-15")
    assert r.status == "SKIPPED", "la identidad del corte es su hash, no su ruta"
    assert _snapshot_state(db) == before


def test_mismo_nombre_con_contenido_distinto_se_procesa(db, cfg, tmp_path, load):
    path = tmp_path / "movimientos.parquet"
    pd.DataFrame([row(amount=100.0)], columns=COLUMNS).to_parquet(path, index=False)
    load(path, "2024-09-15")

    # El origen reenvia el mismo nombre con el monto corregido.
    pd.DataFrame([row(amount=250.0)], columns=COLUMNS).to_parquet(path, index=False)
    r = load(path, "2024-09-16")

    assert r.status == "SUCCESS"
    assert r.counts["UPDATED"] == 1
    assert db.scalar("SELECT count(*) FROM file_registry") == 2, \
        "quedan registrados los dos contenidos"
    assert float(db.scalar("SELECT amount FROM movements_current")) == 250.0


def test_force_permite_reprocesar_sin_duplicar_estado(db, cfg, make_parquet, load):
    path = make_parquet([row()], name="t.parquet")
    load(path, "2024-09-15")
    before = _snapshot_state(db)

    r = load(path, "2024-09-15", force=True)
    assert r.status == "SUCCESS"
    after = _snapshot_state(db)
    # Se ejecuta de verdad, pero el contenido es identico: todo queda UNCHANGED.
    assert r.counts["UNCHANGED"] == 1
    assert r.counts["NEW"] == r.counts["UPDATED"] == r.counts["DELETED"] == 0
    assert after["current"] == before["current"]
    assert after["history"] == before["history"]
    assert after["max_version"] == before["max_version"]
    assert after["amount"] == before["amount"]


def test_force_repetido_genera_run_ids_unicos(db, cfg, make_parquet, load):
    path = make_parquet([row()], name="t.parquet")
    runs = [load(path, "2024-09-15").run_id]
    runs += [load(path, "2024-09-15", force=True).run_id for _ in range(4)]

    assert len(set(runs)) == 5
    assert runs[1:] == [f"{runs[0]}__{n}" for n in range(2, 6)]
    assert db.scalar("SELECT count(*) FROM pipeline_runs") == 5


def test_reprocesar_toda_la_secuencia_es_idempotente(db, cfg, make_parquet, load):
    t = make_parquet([row(id_cliente="CLI000001", amount=100.0),
                      row(id_cliente="CLI000002", amount=200.0)], name="t.parquet")
    t1 = make_parquet([row(id_cliente="CLI000001", amount=150.0),
                       row(id_cliente="CLI000003", amount=300.0)], name="t1.parquet")
    load(t, "2024-09-15")
    load(t1, "2024-09-16")
    before = _snapshot_state(db)

    load(t, "2024-09-15")
    load(t1, "2024-09-16")
    assert _snapshot_state(db) == before


def test_el_run_id_original_sobrevive_a_omisiones_repetidas(db, cfg, make_parquet, load):
    """El INSERT OR REPLACE de la rama SKIPPED no debe pisar el run_id.

    Reescribia file_registry entero con run_id=NULL en cada omision: el
    primer SKIPPED conservaba el run_id de la carga real, pero el segundo ya
    no tenia nada que copiar y lo dejaba en NULL para siempre.
    """
    path = make_parquet([row()], name="t.parquet")
    r0 = load(path, "2024-09-15")

    load(path, "2024-09-15")
    load(path, "2024-09-15")
    r3 = load(path, "2024-09-15")

    assert r3.run_id == r0.run_id
    stored = db.scalar("SELECT run_id FROM file_registry WHERE source_file = 't.parquet'")
    assert stored == r0.run_id


def test_un_corte_anterior_al_ultimo_aplicado_se_rechaza(db, cfg, make_parquet, load):
    import pytest
    from src.ingestion import CriticalIngestionError

    load(make_parquet([row(id_cliente="CLI000001")], name="t1.parquet"), "2024-09-16")
    with pytest.raises(CriticalIngestionError) as e:
        load(make_parquet([row(id_cliente="CLI000002")], name="t.parquet"), "2024-09-15")
    assert e.value.code == "FILE_OUT_OF_ORDER"
