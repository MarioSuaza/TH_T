"""Ejes temporales del tablero.

DuckDB devuelve las columnas DATE como `datetime64[us]`, con una hora 00:00:00
que no significa nada. Streamlit interpreta ese timestamp como un instante y
rotula el eje por hora: con 32 dias de datos el eje mostraba 32 marcas de
"12 PM" y ninguna fecha. Estas pruebas fijan la conversion a texto AAAA-MM-DD
y la marca de dia parcial, que son lo que hace legibles las graficas.
"""

from __future__ import annotations

import pandas as pd

from src.analytics import create_views
from src.presentation import en_miles_de_millones, por_fecha
from tests.conftest import row


def test_por_fecha_convierte_timestamps_en_dias_legibles():
    """El indice debe ser la fecha, no un instante con hora."""
    df = pd.DataFrame({"movement_date": pd.to_datetime(["2024-09-15", "2024-09-16"]),
                       "movements": [1232, 1515]})

    indexado = por_fecha(df, "movement_date")

    assert list(indexado.index) == ["2024-09-15", "2024-09-16"]
    assert indexado.index.dtype == object, \
        "un indice datetime hace que Streamlit rotule por hora, no por dia"


def test_por_fecha_no_altera_los_valores():
    """La conversion es solo del indice: los datos no se tocan."""
    df = pd.DataFrame({"snapshot_date": pd.to_datetime(["2024-10-15"]),
                       "rows_read": [50000]})

    assert por_fecha(df, "snapshot_date")["rows_read"].tolist() == [50000]


def test_por_fecha_no_muta_el_dataframe_original():
    """Devuelve una copia: el DataFrame de origen se sigue pudiendo reutilizar."""
    df = pd.DataFrame({"movement_date": pd.to_datetime(["2024-09-15"]),
                       "movements": [1232]})

    por_fecha(df, "movement_date")

    assert pd.api.types.is_datetime64_any_dtype(df["movement_date"]), \
        "el original debe conservar su tipo fecha"


def test_escala_a_miles_de_millones():
    """Un eje con '20,000,000,000' es ilegible de un vistazo."""
    df = pd.DataFrame({"amount_in": [1.529511e10], "amount_out": [1.326435e10]})

    escalado = en_miles_de_millones(df)

    assert round(float(escalado["amount_in"].iloc[0]), 2) == 15.30
    assert round(float(escalado["amount_out"].iloc[0]), 2) == 13.26


def test_el_ultimo_dia_incompleto_se_marca_como_parcial(db, cfg, load, make_parquet):
    """El dia en curso tiene una fraccion del volumen: no es una caida real."""
    completos = [row(id_cliente=f"CLI{i:06d}", d="2024-09-15") for i in range(1, 41)]
    completos += [row(id_cliente=f"CLI{i:06d}", d="2024-09-16") for i in range(41, 81)]
    parcial = [row(id_cliente=f"CLI{i:06d}", d="2024-09-17") for i in range(81, 84)]

    load(make_parquet(completos + parcial), "2024-09-17")
    create_views(db, cfg)

    diario = db.df("SELECT movement_date, movements, is_partial "
                   "FROM v_daily_movements ORDER BY 1")

    marcados = diario[diario["is_partial"]]["movement_date"].astype(str).tolist()
    assert marcados == ["2024-09-17"], \
        "solo el ultimo dia, y solo si su volumen cae por debajo de la mitad"


def test_un_valle_intermedio_no_se_marca_como_parcial(db, cfg, load, make_parquet):
    """Un domingo flojo en mitad de la serie es un dato real, no un dia en curso."""
    filas = [row(id_cliente=f"CLI{i:06d}", d="2024-09-15") for i in range(1, 41)]
    filas += [row(id_cliente=f"CLI{i:06d}", d="2024-09-16") for i in range(41, 44)]
    filas += [row(id_cliente=f"CLI{i:06d}", d="2024-09-17") for i in range(44, 84)]

    load(make_parquet(filas), "2024-09-17")
    create_views(db, cfg)

    diario = db.df("SELECT movement_date, is_partial FROM v_daily_movements ORDER BY 1")

    assert not diario["is_partial"].any(), \
        "el valle no es el ultimo dia, asi que no se marca"


def test_un_ultimo_dia_completo_no_se_marca(db, cfg, load, make_parquet):
    """La marca exige volumen bajo, no basta con ser el ultimo dia."""
    filas = [row(id_cliente=f"CLI{i:06d}", d="2024-09-15") for i in range(1, 41)]
    filas += [row(id_cliente=f"CLI{i:06d}", d="2024-09-16") for i in range(41, 81)]

    load(make_parquet(filas), "2024-09-16")
    create_views(db, cfg)

    diario = db.df("SELECT is_partial FROM v_daily_movements")

    assert not diario["is_partial"].any()
