"""Clasificacion NEW / UPDATED / DELETED / UNCHANGED entre cortes."""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import COLUMNS, row


def _counts(result):
    return {k: v for k, v in result.counts.items() if v}


def _changes(db):
    return dict(db.execute(
        "SELECT change_type, count(*) FROM movement_changes GROUP BY 1").fetchall())


# ------------------------------------------------------------------ primera carga
def test_primera_carga_todo_es_nuevo(db, cfg, make_parquet, load):
    rows = [row(id_cliente=f"CLI00000{i}") for i in range(1, 4)]
    r = load(make_parquet(rows, name="s1.parquet"), "2024-09-15")
    assert r.status == "SUCCESS"
    assert r.counts["NEW"] == 3
    assert r.counts["UPDATED"] == r.counts["DELETED"] == r.counts["UNCHANGED"] == 0
    assert db.scalar("SELECT count(*) FROM movements_current WHERE is_active") == 3


# ------------------------------------------------------------- los cuatro casos
def test_los_cuatro_casos_del_enunciado(db, cfg, make_parquet, load):
    t = [
        row(id_cliente="CLI000001", amount=100.0),   # se mantiene igual
        row(id_cliente="CLI000002", amount=200.0),   # se corrige
        row(id_cliente="CLI000003", amount=300.0),   # desaparece
    ]
    t1 = [
        row(id_cliente="CLI000001", amount=100.0),
        row(id_cliente="CLI000002", amount=275.0),
        row(id_cliente="CLI000004", amount=400.0),   # aparece
    ]
    load(make_parquet(t, name="t.parquet"), "2024-09-15")
    r = load(make_parquet(t1, name="t1.parquet"), "2024-09-16")

    assert r.counts["UNCHANGED"] == 1
    assert r.counts["UPDATED"] == 1
    assert r.counts["DELETED"] == 1
    assert r.counts["NEW"] == 1
    assert _changes(db) == {"NEW": 4, "UPDATED": 1, "DELETED": 1}


def test_el_registro_corregido_identifica_las_columnas_modificadas(db, cfg,
                                                                   make_parquet, load):
    load(make_parquet([row(amount=250.00, description="Retiro parcial")],
                      name="t.parquet"), "2024-09-15")
    load(make_parquet([row(amount=275.00, description="Retiro total")],
                      name="t1.parquet"), "2024-09-16")

    change = db.execute(
        "SELECT changed_columns, amount_before, amount_after, amount_delta "
        "FROM movement_changes WHERE change_type = 'UPDATED'").fetchone()
    assert sorted(change[0]) == ["amount", "description"]
    assert (float(change[1]), float(change[2]), float(change[3])) == (250.0, 275.0, 25.0)

    fields = dict(db.execute(
        "SELECT column_name, old_value || ' -> ' || new_value "
        "FROM movement_change_fields").fetchall())
    assert fields["amount"] == "250.00 -> 275.00"
    assert fields["description"] == "Retiro parcial -> Retiro total"


def test_case_de_amount_delta_conserva_el_tipo_persistido(db, cfg):
    """Las ramas explicitas evitan DECIMAL(p+1,s) -> DECIMAL(p,s) al insertar."""
    amount_type = cfg.amount_sql_type
    inferred = db.scalar(f"""
        SELECT typeof(CASE
            WHEN change_type IN ('NEW', 'REACTIVATED')
                THEN CAST(amount AS {amount_type})
            WHEN change_type = 'UPDATED'
                THEN CAST(coalesce(amount, 0) - coalesce(cur_amount, 0) AS {amount_type})
            WHEN change_type = 'DELETED'
                THEN CAST(-coalesce(cur_amount, 0) AS {amount_type})
        END)
        FROM (SELECT 'NEW' AS change_type,
                     CAST(1 AS {amount_type}) AS amount,
                     CAST(2 AS {amount_type}) AS cur_amount)
    """)
    assert inferred == amount_type


@pytest.mark.parametrize("field, old, new", [
    ("amount", 100.0, 101.0),
    ("description", "Deposito inicial", "Compra de activo"),
    ("commercial_name", "Bancolombia", "Davivienda"),
])
def test_cambio_de_una_sola_columna(db, cfg, make_parquet, load, field, old, new):
    key = {"amount": "amount", "description": "description",
           "commercial_name": "commercial_name"}[field]
    load(make_parquet([row(**{key: old})], name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(**{key: new})], name="t1.parquet"), "2024-09-16")
    assert r.counts["UPDATED"] == 1
    assert db.scalar(
        "SELECT changed_columns FROM movement_changes WHERE change_type='UPDATED'") == [field]


# ------------------------------------------------------------------ nulos
def test_de_nulo_a_valor_es_un_cambio(db, cfg, make_parquet, load):
    load(make_parquet([row(description=None)], name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(description="Compra de activo")], name="t1.parquet"),
             "2024-09-16")
    assert r.counts["UPDATED"] == 1
    f = db.execute("SELECT column_name, old_value, new_value "
                   "FROM movement_change_fields").fetchone()
    assert f == ("description", None, "Compra de activo")


def test_de_valor_a_nulo_es_un_cambio(db, cfg, make_parquet, load):
    load(make_parquet([row(description="Compra de activo")], name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(description=None)], name="t1.parquet"), "2024-09-16")
    assert r.counts["UPDATED"] == 1


def test_nulo_en_ambos_cortes_no_es_un_cambio(db, cfg, make_parquet, load):
    # El segundo movimiento existe solo para que los dos archivos tengan
    # contenido distinto; si fueran identicos el pipeline los omitiria por
    # idempotencia y la prueba no comprobaria nada.
    load(make_parquet([row(id_cliente="CLI000001", description=None, commercial_name=None),
                       row(id_cliente="CLI000002", amount=1.0)],
                      name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(id_cliente="CLI000001", description=None, commercial_name=None),
                           row(id_cliente="CLI000002", amount=2.0)],
                          name="t1.parquet"), "2024-09-16")
    assert r.counts["UNCHANGED"] == 1, "el movimiento con nulos en ambos cortes no cambia"
    assert r.counts["UPDATED"] == 1, "solo cambia el segundo movimiento"


# --------------------------------------------------------- estabilidad del hash
def test_reordenar_filas_no_produce_cambios(db, cfg, make_parquet, load, tmp_path):
    rows = [row(id_cliente=f"CLI00000{i}", amount=100.0 * i) for i in range(1, 6)]
    a = tmp_path / "t.parquet"
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(a, index=False)
    b = tmp_path / "t1.parquet"
    pd.DataFrame(list(reversed(rows)), columns=COLUMNS).to_parquet(b, index=False)

    load(a, "2024-09-15")
    r = load(b, "2024-09-16")
    assert r.counts["UNCHANGED"] == 5
    assert r.counts["UPDATED"] == r.counts["NEW"] == r.counts["DELETED"] == 0


def test_reordenar_columnas_no_produce_cambios(db, cfg, make_parquet, load, tmp_path):
    rows = [row(id_cliente=f"CLI00000{i}") for i in range(1, 4)]
    a = tmp_path / "t.parquet"
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(a, index=False)
    b = tmp_path / "t1.parquet"
    pd.DataFrame(rows, columns=COLUMNS)[list(reversed(COLUMNS))].to_parquet(b, index=False)

    load(a, "2024-09-15")
    r = load(b, "2024-09-16")
    assert r.counts["UNCHANGED"] == 3


def test_cambiar_solo_el_formato_de_origen_no_produce_cambios(db, cfg, make_parquet, load):
    load(make_parquet([row(d="2024-09-15", type_="entrada", fund="Renta Fija")],
                      name="t.parquet"), "2024-09-15")
    r = load(make_parquet([row(d="15/09/2024", type_="ENTRADA", fund=" renta  fija ")],
                          name="t1.parquet"), "2024-09-16")
    assert r.counts["UNCHANGED"] == 1, ("un cambio de formato en el origen no es una "
                                        "correccion del movimiento")


# ------------------------------------------------------------------ T+2 y T+n
def test_tres_cortes_consecutivos(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001", amount=100.0)], name="t.parquet"),
         "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001", amount=150.0),
                       row(id_cliente="CLI000002", amount=200.0)], name="t1.parquet"),
         "2024-09-16")
    r3 = load(make_parquet([row(id_cliente="CLI000001", amount=150.0),
                            row(id_cliente="CLI000003", amount=300.0)], name="t2.parquet"),
              "2024-09-17")
    assert r3.counts["UNCHANGED"] == 1
    assert r3.counts["NEW"] == 1
    assert r3.counts["DELETED"] == 1
    assert db.scalar("SELECT max(version) FROM movements_history "
                     "WHERE movement_key = (SELECT movement_key FROM movements_current "
                     "                      WHERE id_cliente = 'CLI000001')") == 2


def test_reaparicion_tras_una_baja_es_reactivacion(db, cfg, make_parquet, load):
    load(make_parquet([row(id_cliente="CLI000001")], name="t.parquet"), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000002")], name="t1.parquet"), "2024-09-16")
    r3 = load(make_parquet([row(id_cliente="CLI000001"), row(id_cliente="CLI000002")],
                           name="t2.parquet"), "2024-09-17")
    assert r3.counts["REACTIVATED"] == 1
    assert db.scalar("SELECT count(*) FROM movements_current "
                     "WHERE id_cliente = 'CLI000001' AND is_active") == 1
