"""Validaciones de archivo, esquema y registro."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion import (CriticalIngestionError, read_metadata,
                           resolve_snapshot_date, validate_schema)
from src.normalization import build_staging
from tests.conftest import COLUMNS, row

SNAP = date(2024, 9, 15)


def _stage(db, cfg, path: Path, run_id="r"):
    return build_staging(db, cfg, path=str(path), run_id=run_id,
                         source_file=path.name, source_file_hash="h" + run_id,
                         snapshot_date=SNAP)


def _rejected(db, run_id="r"):
    return dict(db.execute(
        "SELECT error_code, count(*) FROM rejected_records WHERE run_id = ? GROUP BY 1",
        [run_id]).fetchall())


def _flags(db, run_id="r"):
    return dict(db.execute(
        "SELECT flag_code, count(*) FROM data_quality_flags WHERE run_id = ? GROUP BY 1",
        [run_id]).fetchall())


# ------------------------------------------------------------ nivel de archivo
def test_archivo_valido_se_lee(cfg, make_parquet):
    meta = read_metadata(make_parquet([row()]), cfg)
    assert meta.row_count == 1
    assert len(meta.sha256) == 64
    assert set(COLUMNS) <= set(meta.column_names)


def test_archivo_inexistente_es_critico(cfg, tmp_path):
    with pytest.raises(CriticalIngestionError) as e:
        read_metadata(tmp_path / "no_existe.parquet", cfg)
    assert e.value.code == "FILE_NOT_FOUND"


def test_archivo_vacio_es_critico(cfg, make_parquet):
    with pytest.raises(CriticalIngestionError) as e:
        read_metadata(make_parquet([]), cfg)
    assert e.value.code == "FILE_EMPTY"


def test_archivo_ilegible_es_critico(cfg, tmp_path):
    bad = tmp_path / "corrupto.parquet"
    bad.write_bytes(b"esto no es un parquet")
    with pytest.raises(CriticalIngestionError) as e:
        read_metadata(bad, cfg)
    assert e.value.code == "FILE_UNREADABLE"


def test_hash_es_estable_y_distingue_contenido(cfg, make_parquet):
    a = read_metadata(make_parquet([row()], name="a.parquet"), cfg)
    b = read_metadata(make_parquet([row()], name="b.parquet"), cfg)
    c = read_metadata(make_parquet([row(amount=999)], name="c.parquet"), cfg)
    assert a.sha256 == b.sha256      # mismo contenido, distinto nombre
    assert a.sha256 != c.sha256


# ------------------------------------------------------------- nivel de esquema
def test_columna_obligatoria_faltante_detiene_el_pipeline(cfg, tmp_path):
    path = tmp_path / "sin_amount.parquet"
    cols = [c for c in COLUMNS if c != "amount"]
    pd.DataFrame([{k: v for k, v in row().items() if k != "amount"}],
                 columns=cols).to_parquet(path, index=False)
    meta = read_metadata(path, cfg)
    with pytest.raises(CriticalIngestionError) as e:
        validate_schema(meta, cfg)
    assert e.value.code == "MISSING_REQUIRED_COLUMN"
    assert "amount" in str(e.value)


def test_columna_adicional_no_bloquea(cfg, tmp_path):
    path = tmp_path / "extra.parquet"
    r = row() | {"columna_nueva": "x"}
    pd.DataFrame([r], columns=COLUMNS + ["columna_nueva"]).to_parquet(path, index=False)
    meta = read_metadata(path, cfg)
    validate_schema(meta, cfg)          # no lanza
    assert any(i.startswith("EXTRA_COLUMNS") for i in meta.issues)


def test_orden_distinto_de_columnas_no_bloquea(cfg, tmp_path, db):
    path = tmp_path / "orden.parquet"
    inverted = list(reversed(COLUMNS))
    pd.DataFrame([row()], columns=COLUMNS)[inverted].to_parquet(path, index=False)
    meta = read_metadata(path, cfg)
    validate_schema(meta, cfg)
    m = _stage(db, cfg, path)
    assert m["rows_valid"] == 1


def test_orden_de_columnas_no_altera_el_row_hash(cfg, tmp_path, db):
    normal = tmp_path / "n.parquet"
    pd.DataFrame([row()], columns=COLUMNS).to_parquet(normal, index=False)
    inverted = tmp_path / "i.parquet"
    pd.DataFrame([row()], columns=COLUMNS)[list(reversed(COLUMNS))].to_parquet(inverted, index=False)
    _stage(db, cfg, normal, "a")
    _stage(db, cfg, inverted, "b")
    hashes = db.execute(
        "SELECT DISTINCT row_hash FROM stg_movements").fetchall()
    assert len(hashes) == 1


def test_orden_de_filas_no_altera_los_hashes(cfg, tmp_path, db):
    rows = [row(id_cliente="CLI000001"), row(id_cliente="CLI000002"),
            row(id_cliente="CLI000003")]
    a = tmp_path / "a.parquet"
    pd.DataFrame(rows, columns=COLUMNS).to_parquet(a, index=False)
    b = tmp_path / "b.parquet"
    pd.DataFrame(list(reversed(rows)), columns=COLUMNS).to_parquet(b, index=False)
    _stage(db, cfg, a, "a")
    _stage(db, cfg, b, "b")
    ka = {r[0] for r in db.execute("SELECT movement_key FROM stg_movements WHERE run_id='a'").fetchall()}
    kb = {r[0] for r in db.execute("SELECT movement_key FROM stg_movements WHERE run_id='b'").fetchall()}
    assert ka == kb


# ------------------------------------------------------------ nivel de registro
@pytest.mark.parametrize("bad_row, code", [
    (row(id_cliente=None), "NULL_ID"),
    (row(id_cliente="   "), "EMPTY_ID"),
    (row(d=None), "NULL_DATE"),
    (row(d="2024-13-45"), "INVALID_DATE"),
    (row(d="no es una fecha"), "INVALID_DATE"),
    (row(type_=None), "NULL_TYPE"),
    (row(type_="transferencia"), "UNKNOWN_TYPE"),
    (row(fund=None), "NULL_FUND"),
    (row(product=None), "NULL_PRODUCT"),
    (row(amount=None), "NULL_AMOUNT"),
    (row(amount=float("nan")), "NULL_AMOUNT"),
    (row(amount=float("inf")), "NON_FINITE_AMOUNT"),
])
def test_registros_invalidos_van_a_cuarentena(db, cfg, make_parquet, bad_row, code):
    m = _stage(db, cfg, make_parquet([bad_row]))
    assert m["rows_valid"] == 0
    assert m["rows_rejected"] == 1
    assert code in _rejected(db)


def test_la_cuarentena_conserva_la_fila_original(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(amount=None, description="Retiro parcial")]))
    raw = db.scalar("SELECT raw_record FROM rejected_records")
    assert "CLI000001" in raw and "Retiro parcial" in raw


def test_ningun_registro_desaparece_en_silencio(db, cfg, make_parquet):
    rows = [row(id_cliente=f"CLI00000{i}") for i in range(1, 5)]
    rows[1]["amount"] = None
    rows[3]["type"] = "desconocido"
    m = _stage(db, cfg, make_parquet(rows))
    assert m["rows_valid"] + m["rows_rejected"] == len(rows)


# ---------------------------------------------------------------- normalizacion
@pytest.mark.parametrize("raw_type, expected", [
    ("entrada", "IN"), ("Entrada", "IN"), ("ENTRADA", "IN"), ("in", "IN"), ("IN", "IN"),
    ("salida", "OUT"), ("Salida", "OUT"), ("SALIDA", "OUT"), ("out", "OUT"), ("OUT", "OUT"),
])
def test_las_diez_variantes_de_type_se_canonicalizan(db, cfg, make_parquet,
                                                     raw_type, expected):
    _stage(db, cfg, make_parquet([row(type_=raw_type)]))
    assert db.scalar("SELECT type FROM stg_movements") == expected
    assert db.scalar("SELECT type_original FROM stg_movements") == raw_type


@pytest.mark.parametrize("raw_fund", [
    "Renta Fija", "renta fija", "RENTA FIJA", "  Renta Fija  ", "Renta  Fija"])
def test_las_variantes_de_fund_se_canonicalizan(db, cfg, make_parquet, raw_fund):
    _stage(db, cfg, make_parquet([row(fund=raw_fund)]))
    assert db.scalar("SELECT fund FROM stg_movements") == "Renta Fija"


@pytest.mark.parametrize("raw_date", ["2024-09-15", "15/09/2024"])
def test_ambos_formatos_de_fecha_dan_la_misma_fecha(db, cfg, make_parquet, raw_date):
    _stage(db, cfg, make_parquet([row(d=raw_date)]))
    assert str(db.scalar("SELECT movement_date FROM stg_movements")) == "2024-09-15"


def test_los_dos_formatos_de_fecha_producen_la_misma_clave(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(d="2024-09-15")]), "iso")
    _stage(db, cfg, make_parquet([row(d="15/09/2024")]), "dmy")
    keys = db.execute("SELECT DISTINCT movement_key FROM stg_movements").fetchall()
    assert len(keys) == 1, "el formato de origen no debe afectar a la identidad"


def test_espacios_externos_se_recortan_sin_alterar_el_significado(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(description="  Retiro parcial  ",
                                      commercial_name=" Banco  X ")]))
    assert db.scalar("SELECT description FROM stg_movements") == "Retiro parcial"
    assert db.scalar("SELECT commercial_name FROM stg_movements") == "Banco X"


def test_el_valor_original_siempre_se_conserva(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(type_="ENTRADA", fund="  renta  fija ")]))
    r = db.execute("SELECT type, type_original, fund, fund_original FROM stg_movements").fetchone()
    assert r == ("IN", "ENTRADA", "Renta Fija", "  renta  fija ")


def test_el_monto_se_persiste_como_decimal_exacto(db, cfg, make_parquet):
    from decimal import Decimal
    _stage(db, cfg, make_parquet([row(amount=1490831.17)]))
    value = db.scalar("SELECT amount FROM stg_movements")
    assert isinstance(value, Decimal)
    assert value == Decimal("1490831.17")


def test_la_suma_de_decimales_es_exacta(db, cfg, make_parquet):
    from decimal import Decimal
    rows = [row(id_cliente=f"CLI00{i:04d}", amount=0.10) for i in range(1, 11)]
    _stage(db, cfg, make_parquet(rows))
    assert db.scalar("SELECT sum(amount) FROM stg_movements") == Decimal("1.00")


@pytest.mark.parametrize("raw_amount, code", [
    ("1.234,56", "INVALID_AMOUNT_FORMAT"),
    ("no-es-numero", "INVALID_AMOUNT_FORMAT"),
    (1e30, "AMOUNT_OUT_OF_RANGE"),
])
def test_montos_invalidos_se_clasifican_sin_abortar_el_corte(
        db, cfg, make_parquet, raw_amount, code):
    m = _stage(db, cfg, make_parquet([row(amount=raw_amount)]))
    assert m["rows_valid"] == 0 and m["rows_rejected"] == 1
    assert code in _rejected(db)


def test_monto_textual_canonico_se_acepta(db, cfg, make_parquet):
    from decimal import Decimal

    m = _stage(db, cfg, make_parquet([row(amount="1234.56")]))
    assert m["rows_valid"] == 1
    assert db.scalar("SELECT amount FROM stg_movements") == Decimal("1234.56")


# ------------------------------------------------------------- flags de aviso
def test_los_montos_negativos_se_marcan_pero_no_se_rechazan(db, cfg, make_parquet):
    m = _stage(db, cfg, make_parquet([row(amount=-500.00)]))
    assert m["rows_valid"] == 1 and m["rows_rejected"] == 0
    assert "NEGATIVE_AMOUNT" in _flags(db)


def test_un_fondo_desconocido_se_marca_pero_no_se_rechaza(db, cfg, make_parquet):
    m = _stage(db, cfg, make_parquet([row(fund="Fondo Nuevo 2026")]))
    assert m["rows_valid"] == 1
    assert "UNKNOWN_FUND" in _flags(db)
    assert db.scalar("SELECT fund FROM stg_movements") == "Fondo Nuevo 2026"


def test_un_id_con_formato_atipico_se_marca_pero_no_se_rechaza(db, cfg, make_parquet):
    m = _stage(db, cfg, make_parquet([row(id_cliente="X-1")]))
    assert m["rows_valid"] == 1
    assert "INVALID_ID_FORMAT" in _flags(db)


def test_una_fecha_posterior_al_corte_se_marca(db, cfg, make_parquet):
    _stage(db, cfg, make_parquet([row(d="2025-01-01")]))
    assert "DATE_OUT_OF_RANGE" in _flags(db)


# ------------------------------------------------------- fecha del corte
def test_la_fecha_de_cli_tiene_prioridad(cfg, make_parquet):
    meta = read_metadata(make_parquet([row()], name="movimientos_2024-01-01.parquet"), cfg)
    resolve_snapshot_date(meta, cfg, date(2030, 5, 5))
    assert meta.snapshot_date == date(2030, 5, 5)
    assert meta.snapshot_date_source == "cli"


def test_la_fecha_se_deduce_del_nombre_si_no_hay_cli(cfg, make_parquet):
    meta = read_metadata(make_parquet([row()], name="movimientos_2026-08-05.parquet"), cfg)
    resolve_snapshot_date(meta, cfg, None)
    assert meta.snapshot_date == date(2026, 8, 5)
    assert meta.snapshot_date_source == "filename_iso"


def test_sin_fecha_se_infiere_del_contenido(cfg, make_parquet):
    path = make_parquet([row(d="2024-09-15"), row(id_cliente="CLI000002", d="2024-09-20")],
                        name="sin_fecha_en_el_nombre.parquet")
    meta = read_metadata(path, cfg)
    resolve_snapshot_date(meta, cfg, None)
    assert meta.snapshot_date == date(2024, 9, 20)
    assert meta.snapshot_date_source == "max_content_date"
