"""Adaptaciones de datos para su presentacion en graficas.

Vive en `src/` y no en `dashboard/` a proposito: son transformaciones de pandas
sin dependencia de Streamlit, asi que se pueden probar sin instalarlo. El
tablero las importa; las pruebas tambien.
"""

from __future__ import annotations

import pandas as pd

# Un eje con "20,000,000,000" es ilegible de un vistazo. Los montos de este
# conjunto se mueven en el orden de 1e10, asi que la escala natural es esta.
MIL_MILLONES = 1e9


def por_fecha(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    """Indexa por fecha en formato AAAA-MM-DD, como texto.

    DuckDB devuelve las columnas DATE como `datetime64[us]`, con una hora
    00:00:00 que no significa nada. Streamlit ve un timestamp y rotula el eje
    por hora: con 32 dias salian 32 marcas de "12 PM" y ninguna fecha legible.

    Un indice de texto ya ordenado se rotula como categoria y muestra el dia.
    Devuelve una copia: el DataFrame de origen conserva sus tipos.
    """
    salida = df.copy()
    salida[columna] = pd.to_datetime(salida[columna]).dt.strftime("%Y-%m-%d")
    return salida.set_index(columna)


def en_miles_de_millones(df: pd.DataFrame) -> pd.DataFrame:
    """Reescala los montos para que el eje sea legible."""
    return df / MIL_MILLONES
