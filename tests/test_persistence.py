"""Estado vigente, historico SCD2, eliminacion logica y bitacora."""

from __future__ import annotations

from src.persistence import check_invariants
from tests.conftest import row


def test_maximo_una_fila_vigente_por_movimiento(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    load(make_parquet([row(amount=300.0)], name="t2.parquet"), "2024-09-17")

    assert db.scalar("SELECT count(*) FROM movements_current") == 1
    assert db.scalar(
        "SELECT count(*) FROM (SELECT movement_key FROM movements_current "
        "GROUP BY 1 HAVING count(*) > 1)") == 0


def test_el_historico_guarda_una_version_por_correccion(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    load(make_parquet([row(amount=300.0)], name="t2.parquet"), "2024-09-17")

    hist = db.df("SELECT version, amount, change_type, valid_from, valid_to, is_current "
                 "FROM movements_history ORDER BY version")
    assert list(hist["version"]) == [1, 2, 3]
    assert [float(a) for a in hist["amount"]] == [100.0, 200.0, 300.0]
    assert list(hist["change_type"]) == ["NEW", "UPDATED", "UPDATED"]
    assert list(hist["is_current"]) == [False, False, True]


def test_las_versiones_no_se_solapan_en_el_tiempo(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    hist = db.df("SELECT version, valid_from, valid_to FROM movements_history ORDER BY version")
    assert str(hist.iloc[0].valid_to)[:10] == "2024-09-16"
    assert str(hist.iloc[1].valid_from)[:10] == "2024-09-16"
    assert hist.iloc[1].valid_to is None or str(hist.iloc[1].valid_to) == "NaT"


def test_una_sola_version_marcada_como_vigente(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    assert db.scalar("SELECT count(*) FROM movements_history WHERE is_current") == 1


def test_la_baja_es_logica_y_conserva_el_registro(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002")],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000002")], name="t1.parquet"), "2024-09-16")

    assert db.scalar("SELECT count(*) FROM movements_current") == 2, "no hay borrado fisico"
    r = db.execute("SELECT is_active, deleted_at FROM movements_current "
                   "WHERE id_cliente = 'CLI000001'").fetchone()
    assert r[0] is False and r[1] is not None


def test_la_baja_cierra_la_version_historica_sin_crear_una_vacia(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002")],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000002")], name="t1.parquet"), "2024-09-16")

    hist = db.execute(
        "SELECT count(*), max(CAST(is_deleted AS INT)), max(CAST(valid_to AS VARCHAR)) "
        "FROM movements_history h JOIN movements_current c USING (movement_key) "
        "WHERE c.id_cliente = 'CLI000001'").fetchone()
    assert hist[0] == 1, "la baja no crea una version adicional"
    assert hist[1] == 1, "la version queda marcada como eliminada"
    assert hist[2][:10] == "2024-09-16"


def test_la_reactivacion_abre_una_version_nueva(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002")],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000002")], name="t1.parquet"), "2024-09-16")
    # CLI000001 vuelve a aparecer tras haber sido dado de baja.
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002")],
                      name="t2.parquet"), "2024-09-17")

    versions = db.df(
        "SELECT h.version, h.change_type, h.is_current FROM movements_history h "
        "JOIN movements_current c USING (movement_key) "
        "WHERE c.id_cliente = 'CLI000001' ORDER BY h.version")
    assert list(versions["change_type"]) == ["NEW", "REACTIVATED"]
    assert db.scalar("SELECT is_active FROM movements_current "
                     "WHERE id_cliente = 'CLI000001'") is True


def test_sin_cambios_no_crea_version_ni_evento(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002", amount=1.0)],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002", amount=2.0)],
                      name="t1.parquet"), "2024-09-16")

    key = db.scalar("SELECT movement_key FROM movements_current WHERE id_cliente='CLI000001'")
    assert db.scalar("SELECT count(*) FROM movements_history WHERE movement_key = ?", [key]) == 1
    assert db.scalar("SELECT count(*) FROM movement_changes WHERE movement_key = ? "
                     "AND change_type <> 'NEW'", [key]) == 0


def test_sin_cambios_actualiza_solo_la_marca_de_visto(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002", amount=1.0)],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002", amount=2.0)],
                      name="t1.parquet"), "2024-09-16")
    r = db.execute("SELECT first_snapshot_date, last_snapshot_date, version "
                   "FROM movements_current WHERE id_cliente = 'CLI000001'").fetchone()
    assert str(r[0])[:10] == "2024-09-15"
    assert str(r[1])[:10] == "2024-09-16"
    assert r[2] == 1


def test_el_alta_original_se_conserva_tras_correcciones(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    r = db.execute("SELECT first_snapshot_date, last_snapshot_date, version "
                   "FROM movements_current").fetchone()
    assert str(r[0])[:10] == "2024-09-15"
    assert str(r[1])[:10] == "2024-09-16"
    assert r[2] == 2


def test_la_cuarentena_se_escribe_con_trazabilidad(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002", amount=None)],
                      name="t.parquet"), "2024-09-15")
    r = db.execute("SELECT run_id, source_file, source_file_hash, snapshot_date, "
                   "error_code, error_severity FROM rejected_records").fetchone()
    assert all(v is not None for v in r)
    assert r[4] == "NULL_AMOUNT"


def test_las_invariantes_estructurales_se_cumplen(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 6)],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001", amount=999.0),
                       row(id_cliente="CLI000002"),
                       row(id_cliente="CLI000009")], name="t1.parquet"), "2024-09-16")
    assert check_invariants(db) == []


def test_el_movimiento_activo_apunta_a_una_version_abierta(db, cfg, make_parquet, load):
    load(make_parquet([row(amount=100.0)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=200.0)], name="t1.parquet"), "2024-09-16")
    r = db.execute(
        "SELECT c.version, h.version, h.valid_to FROM movements_current c "
        "JOIN movements_history h USING (movement_key) WHERE h.is_current").fetchone()
    assert r[0] == r[1] == 2
    assert r[2] is None
