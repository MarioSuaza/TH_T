"""La clave de negocio del contrato debe gobernar el particionado y el hash reales.

`config/data_contract.yml -> identity.business_key` se documenta como la fuente
de verdad para identificar un movimiento. Antes de este cambio, `sql/staging.sql`
tenia las cinco columnas escritas como literales en el PARTITION BY y en el
concat_ws del hash, asi que cambiar business_key en el YAML no tenia ningun
efecto observable: staging.py:build_staging nunca leia cfg.business_key.
"""

from __future__ import annotations

from tests.conftest import row


def test_reducir_business_key_hace_ambiguas_filas_que_antes_eran_distintas(
        db, cfg, load, make_parquet, monkeypatch):
    """Si la clave de negocio se reduce a [id_cliente, date, product], dos
    filas que solo difieren en `type` deben caer en la MISMA particion
    (is_key_ambiguous = true para ambas, desempatadas por occurrence_ordinal),
    en vez de ser particiones independientes como con la clave por defecto.
    """
    monkeypatch.setitem(cfg._contract["identity"], "business_key",
                        ["id_cliente", "date", "product"])

    load(make_parquet([
        row(type_="entrada", amount=100.00),
        row(type_="salida", amount=200.00),
    ]), "2024-09-15")

    ambiguous = db.execute(
        "SELECT count(*) FROM movements_current WHERE is_key_ambiguous").fetchone()[0]
    assert ambiguous == 2, (
        "con business_key = [id_cliente, date, product], las dos filas "
        "(que solo difieren en type) deben caer en la misma particion y "
        "marcarse como is_key_ambiguous")


def test_business_key_por_defecto_mantiene_movimientos_con_distinto_type_separados(
        db, cfg, load, make_parquet):
    """Caracterizacion del comportamiento por defecto: sin tocar la config,
    dos filas que difieren en `type` no son ambiguas entre si (cada una cae
    en su propia particion de business_key)."""
    load(make_parquet([
        row(type_="entrada", amount=100.00),
        row(type_="salida", amount=200.00),
    ]), "2024-09-15")

    ambiguous = db.execute(
        "SELECT count(*) FROM movements_current WHERE is_key_ambiguous").fetchone()[0]
    assert ambiguous == 0
