"""Normalizacion, trazabilidad y ausencia de falsas bajas.

Estas pruebas fijan tres propiedades que el diagnostico verifico sobre los
archivos reales y que un cambio futuro podria romper en silencio:

1. Las 10 variantes de `type` observadas convergen a IN/OUT.
2. El valor original sobrevive hasta la capa Gold (auditoria).
3. Una diferencia de formato -- en `type` o en `date` -- NO produce una baja
   seguida de un alta: produce UNCHANGED.

La tercera es la mas importante. Si `movement_key` se construyera sobre el
valor sin normalizar, `entrada` -> `ENTRADA` entre dos cortes generaria una
clave distinta y el pipeline reportaria una baja y un alta falsas.
"""

from __future__ import annotations

import pytest

from tests.conftest import row

# Variantes observadas en los archivos entregados, con su canonico esperado.
TYPE_VARIANTS = [
    ("entrada", "IN"), ("Entrada", "IN"), ("ENTRADA", "IN"),
    ("in", "IN"), ("IN", "IN"),
    ("salida", "OUT"), ("Salida", "OUT"), ("SALIDA", "OUT"),
    ("out", "OUT"), ("OUT", "OUT"),
]


@pytest.mark.parametrize("raw,canonical", TYPE_VARIANTS)
def test_type_variants_map_to_canonical(db, load, make_parquet, raw, canonical):
    """Cada forma escrita observada produce el mismo valor canonico."""
    load(make_parquet([row(type_=raw)]), "2024-09-15")

    got = db.df("SELECT type, type_original FROM movements_current").iloc[0]
    assert got["type"] == canonical
    assert got["type_original"] == raw, "el valor original debe conservarse intacto"


def test_type_normalization_is_idempotent(db, load, make_parquet):
    """Normalizar un valor ya canonico no lo altera."""
    load(make_parquet([row(type_="IN")]), "2024-09-15")
    once = db.scalar("SELECT type FROM movements_current")

    load(make_parquet([row(type_=once)], name="second.parquet"), "2024-09-16")
    twice = db.scalar("SELECT type FROM movements_current "
                      "ORDER BY last_snapshot_date DESC LIMIT 1")

    assert once == twice == "IN"


def test_type_case_change_is_not_a_deletion(db, load, make_parquet):
    """`entrada` -> `ENTRADA` entre cortes es UNCHANGED, no DELETED + NEW."""
    load(make_parquet([row(type_="entrada")]), "2024-09-15")
    result = load(make_parquet([row(type_="ENTRADA")], name="t1.parquet"), "2024-09-16")

    assert result.counts["DELETED"] == 0, "una diferencia de mayusculas no es una baja"
    assert result.counts["NEW"] == 0, "tampoco es un alta"
    assert result.counts["UNCHANGED"] == 1


def test_date_format_change_is_not_a_deletion(db, load, make_parquet):
    """`2024-09-15` -> `15/09/2024` es UNCHANGED: ambas resuelven al mismo dia."""
    load(make_parquet([row(d="2024-09-15")]), "2024-09-15")
    result = load(make_parquet([row(d="15/09/2024")], name="t1.parquet"), "2024-09-16")

    assert result.counts["DELETED"] == 0
    assert result.counts["NEW"] == 0
    assert result.counts["UNCHANGED"] == 1


def test_whitespace_in_type_is_not_a_deletion(db, load, make_parquet):
    """Espacios sobrantes tampoco rompen la identidad del movimiento."""
    load(make_parquet([row(type_="entrada")]), "2024-09-15")
    result = load(make_parquet([row(type_="  entrada  ")], name="t1.parquet"),
                  "2024-09-16")

    assert result.counts["DELETED"] == 0
    assert result.counts["UNCHANGED"] == 1


def test_unknown_type_is_quarantined_not_guessed(db, load, make_parquet):
    """Un valor fuera del catalogo va a cuarentena; no se asigna IN u OUT."""
    load(make_parquet([row(type_="transferencia")]), "2024-09-15")

    assert db.scalar("SELECT count(*) FROM movements_current") == 0
    assert db.scalar("SELECT count(*) FROM rejected_records "
                     "WHERE error_code = 'UNKNOWN_TYPE'") == 1


def test_originals_survive_an_update(db, load, make_parquet):
    """Tras una correccion, el original refleja el ultimo valor recibido."""
    load(make_parquet([row(type_="entrada", amount=100.00)]), "2024-09-15")
    load(make_parquet([row(type_="ENTRADA", amount=250.00)], name="t1.parquet"),
         "2024-09-16")

    got = db.df("SELECT type, type_original, amount, amount_original "
                "FROM movements_current").iloc[0]
    assert got["type"] == "IN"
    assert got["type_original"] == "ENTRADA"
    assert float(got["amount"]) == 250.00
    assert float(got["amount_original"]) == 250.00
