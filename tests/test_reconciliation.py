"""Reconciliaciones: conteos, sumas y ausencia de perdidas o duplicados."""

from __future__ import annotations

from decimal import Decimal

from tests.conftest import row


def _checks(db):
    return db.df("SELECT check_group, check_name, left_value, right_value, difference, "
                 "passed FROM reconciliation_results ORDER BY check_group, check_name")


def test_todos_los_controles_cuadran_en_la_primera_carga(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente=f"CLI00000{i}", amount=100.0 * i)
                       for i in range(1, 6)], name="t.parquet"), "2024-09-15")
    checks = _checks(db)
    assert len(checks) > 0
    assert checks["passed"].all(), checks[~checks["passed"]].to_string()


def test_todos_los_controles_cuadran_tras_una_evolucion(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente=f"CLI00000{i}", amount=100.0 * i)
                       for i in range(1, 6)], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001", amount=999.0),   # corregido
                       row(id_cliente="CLI000002", amount=200.0),   # sin cambios
                       row(id_cliente="CLI000009", amount=900.0)],  # nuevo
                      name="t1.parquet"), "2024-09-16")
    checks = _checks(db)
    assert checks["passed"].all(), checks[~checks["passed"]].to_string()


def test_filas_leidas_igual_a_validas_mas_rechazadas(db, cfg, make_parquet, load):
    rows = [row(id_cliente=f"CLI00000{i}") for i in range(1, 6)]
    rows[2]["amount"] = None
    rows[4]["type"] = "desconocido"
    load(make_parquet(rows, name="t.parquet"), "2024-09-15")

    run = db.execute("SELECT rows_read, rows_valid, rows_rejected FROM pipeline_runs "
                     "WHERE status = 'SUCCESS'").fetchone()
    assert run[0] == run[1] + run[2] == 5


def test_la_clasificacion_cubre_todas_las_claves(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 6)],
                      name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(id_cliente="CLI000001", amount=1.0),
                           row(id_cliente="CLI000002"),
                           row(id_cliente="CLI000009")], name="t1.parquet"), "2024-09-16")

    total = sum(r.counts[k] for k in
                ("NEW", "UPDATED", "DELETED", "UNCHANGED", "REACTIVATED", "STILL_DELETED"))
    # 5 claves de T + 1 clave nueva de T+1
    assert total == 6
    assert r.counts["NEW"] + r.counts["UPDATED"] + r.counts["UNCHANGED"] + \
           r.counts["REACTIVATED"] == 3


def test_ninguna_clave_del_corte_se_pierde(db, cfg, make_parquet, load):
    rows = [row(id_cliente=f"CLI0000{i:02d}") for i in range(1, 21)]
    load(make_parquet(rows, name="t.parquet"), "2024-09-15")
    sin_reflejo = db.scalar("""
        SELECT count(*) FROM stg_movements s
        WHERE NOT EXISTS (SELECT 1 FROM movements_current m
                          WHERE m.movement_key = s.movement_key AND m.is_active)""")
    assert sin_reflejo == 0


def test_la_suma_monetaria_se_conserva_exactamente(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001", amount=1234.56),
                       row(id_cliente="CLI000002", amount=0.01),
                       row(id_cliente="CLI000003", amount=-99.99)],
                      name="t.parquet"), "2024-09-15")
    total = db.scalar("SELECT sum(amount) FROM movements_current WHERE is_active")
    assert isinstance(total, Decimal)
    assert total == Decimal("1134.58"), "sin errores de coma flotante"


def test_la_continuidad_monetaria_entre_cortes_cuadra(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001", amount=100.00),
                       row(id_cliente="CLI000002", amount=200.00)],
                      name="t.parquet"), "2024-09-15")
    before = db.scalar("SELECT sum(amount) FROM movements_current WHERE is_active")

    load(make_parquet([row(id_cliente="CLI000001", amount=150.00),   # +50
                       row(id_cliente="CLI000003", amount=75.00)],   # +75, y -200 por la baja
                      name="t1.parquet"), "2024-09-16")
    after = db.scalar("SELECT sum(amount) FROM movements_current WHERE is_active")
    delta = db.scalar("SELECT sum(amount_delta) FROM movement_changes "
                      "WHERE run_id = (SELECT run_id FROM pipeline_runs "
                      "                WHERE snapshot_date = '2024-09-16')")
    assert after == before + delta
    assert after == Decimal("225.00")


def test_no_hay_mas_de_una_fila_vigente_por_clave(db, cfg, make_parquet, load):
    for i, amount in enumerate([100.0, 200.0, 300.0, 400.0], start=15):
        load(make_parquet([row(amount=amount)], name=f"s{i}.parquet"), f"2024-09-{i}")
    assert db.scalar("SELECT count(*) FROM (SELECT movement_key FROM movements_current "
                     "GROUP BY 1 HAVING count(*) > 1)") == 0
    assert db.scalar("SELECT count(*) FROM (SELECT movement_key FROM movements_history "
                     "WHERE is_current GROUP BY 1 HAVING count(*) > 1)") == 0


def test_se_generan_los_reportes_de_reconciliacion(db, cfg, make_parquet, load):
    r = load(make_parquet([row()], name="t.parquet"), "2024-09-15")
    reports = cfg.paths.reports
    assert (reports / f"reconciliation_{r.run_id}.csv").exists()
    assert (reports / f"reconciliation_{r.run_id}.md").exists()
    contenido = (reports / f"reconciliation_{r.run_id}.md").read_text(encoding="utf-8")
    assert "TODOS LOS CONTROLES CUADRAN" in contenido


def test_los_montos_por_dimension_suman_el_total(db, cfg, make_parquet, load):
    load(make_parquet([
        row(id_cliente="CLI000001", fund="Renta Fija", product="Bonos", amount=100.0),
        row(id_cliente="CLI000002", fund="Balanceado", product="ETF", amount=250.5),
        row(id_cliente="CLI000003", fund="Renta Fija", product="ETF", amount=49.5),
    ], name="t.parquet"), "2024-09-15")

    from src.analytics import create_views
    create_views(db, cfg)
    total = db.scalar("SELECT sum(amount) FROM movements_current WHERE is_active")
    assert db.scalar("SELECT sum(amount_total) FROM v_summary_by_fund") == total
    assert db.scalar("SELECT sum(amount_total) FROM v_summary_by_product") == total
    assert db.scalar("SELECT sum(amount_total) FROM v_summary_by_type") == total
