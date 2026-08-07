"""Tratamiento de duplicados dentro de un mismo corte."""

from __future__ import annotations

from datetime import date

from src.normalization import build_staging
from tests.conftest import row

SNAP = date(2024, 9, 15)


def _stage(db, cfg, path, run_id="r"):
    return build_staging(db, cfg, path=str(path), run_id=run_id, source_file=path.name,
                         source_file_hash="h", snapshot_date=SNAP)


def test_duplicado_exacto_se_colapsa_y_se_contabiliza(db, cfg, make_parquet):
    m = _stage(db, cfg, make_parquet([row(), row(), row()]))
    assert m["rows_valid"] == 1, "las tres filas identicas colapsan en una"
    assert m["rows_exact_dupes"] == 2, "se contabilizan las dos eliminadas"
    assert db.scalar("SELECT exact_duplicate_count FROM stg_movements") == 3


def test_duplicado_exacto_no_genera_versiones_historicas(db, cfg, make_parquet, load):
    load(make_parquet([row(), row()], name="s1.parquet"), "2024-09-15")
    assert db.scalar("SELECT count(*) FROM movements_history") == 1
    assert db.scalar("SELECT count(*) FROM movements_current") == 1


def test_duplicado_exacto_con_variantes_sucias_tambien_colapsa(db, cfg, make_parquet):
    # Misma fila escrita con distinto formato de fecha y de type: tras normalizar
    # son identicas, por lo que deben colapsar.
    m = _stage(db, cfg, make_parquet([
        row(d="2024-09-15", type_="entrada", fund="Renta Fija"),
        row(d="15/09/2024", type_="ENTRADA", fund="  renta  fija "),
    ]))
    assert m["rows_valid"] == 1
    assert m["rows_exact_dupes"] == 1


def test_misma_clave_con_datos_distintos_se_desempata_con_ordinal(db, cfg, make_parquet):
    # Dos movimientos legitimos del mismo cliente, mismo dia, mismo producto,
    # fondo y tipo, pero de importe distinto.
    m = _stage(db, cfg, make_parquet([row(amount=100.00), row(amount=200.00)]))
    assert m["rows_valid"] == 2, "no se pierde ninguno"
    assert m["rows_key_ambiguous"] == 2
    ordinals = sorted(r[0] for r in db.execute(
        "SELECT occurrence_ordinal FROM stg_movements").fetchall())
    assert ordinals == [1, 2]
    assert db.scalar("SELECT count(DISTINCT movement_key) FROM stg_movements") == 2


def test_la_clave_ambigua_queda_marcada_y_reportada(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(amount=100.00), row(amount=200.00)]))
    assert db.scalar("SELECT count(*) FROM stg_movements WHERE is_key_ambiguous") == 2
    assert db.scalar(
        "SELECT count(*) FROM data_quality_flags WHERE flag_code = 'KEY_AMBIGUOUS'") == 2


def test_el_ordinal_es_determinista_ante_reordenaciones(db, cfg, make_parquet, tmp_path):
    import pandas as pd
    from tests.conftest import COLUMNS

    rows = [row(amount=300.00), row(amount=100.00), row(amount=200.00)]
    a = tmp_path / "a.parquet"
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(a, index=False)
    b = tmp_path / "b.parquet"
    pd.DataFrame(list(reversed(rows)), columns=COLUMNS).to_parquet(b, index=False)

    _stage(db, cfg, a, "a")
    _stage(db, cfg, b, "b")
    pairs_a = db.execute("SELECT occurrence_ordinal, amount FROM stg_movements "
                         "WHERE run_id='a' ORDER BY 1").fetchall()
    pairs_b = db.execute("SELECT occurrence_ordinal, amount FROM stg_movements "
                         "WHERE run_id='b' ORDER BY 1").fetchall()
    assert pairs_a == pairs_b, "el ordinal depende del contenido, no del orden del archivo"


def test_varias_claves_duplicadas_en_el_mismo_corte(db, cfg, make_parquet):
    rows = []
    for i in (1, 2, 3):
        rows += [row(id_cliente=f"CLI00000{i}", amount=10.0),
                 row(id_cliente=f"CLI00000{i}", amount=20.0)]
    m = _stage(db, cfg, make_parquet(rows))
    assert m["rows_valid"] == 6
    assert m["rows_key_ambiguous"] == 6
    assert db.scalar("SELECT count(DISTINCT movement_key) FROM stg_movements") == 6


def test_misma_business_key_con_datos_distintos_no_va_a_cuarentena(db, cfg, make_parquet):
    """Comportamiento fijo (no configurable): una clave de negocio repetida
    con datos distintos dentro del mismo corte se desempata con el ordinal
    determinista, no se manda a cuarentena. Ver quality.exact_duplicate_policy
    / conflicting_duplicate_policy en config/pipeline.yml -- ambas se
    retiraron del YAML por ser aspiracionales: ningun codigo las leia."""
    m = _stage(db, cfg, make_parquet([row(amount=100.0), row(amount=200.0)]))
    assert m["rows_valid"] == 2
    assert m["rows_rejected"] == 0
    assert m["rows_key_ambiguous"] == 2
