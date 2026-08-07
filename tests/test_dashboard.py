"""Prueba de humo del tablero.

Ejecuta el script de Streamlit de principio a fin contra una base real generada
por el pipeline y comprueba que no lanza ninguna excepcion. Sin esto, un error
en una consulta del tablero solo se descubriria abriendolo a mano.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.conftest import COLUMNS, row

APP = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"

pytest.importorskip("streamlit", reason="streamlit no esta instalado")


@pytest.fixture()
def poblada(tmp_path, monkeypatch):
    """Base con dos cortes que cubren los cuatro tipos de cambio."""
    from src import pipeline as pipeline_mod
    from src.config import load_config

    raw = tmp_path / "raw"
    raw.mkdir()
    base = [row(id_cliente=f"CLI{i:06d}", amount=100.0 + i,
                fund=["Renta Fija", "Balanceado", "Crecimiento"][i % 3],
                product=["Bonos", "ETF", "CDT"][i % 3],
                type_=["entrada", "salida"][i % 2],
                commercial_name=None if i % 7 == 0 else "Bancolombia")
            for i in range(1, 121)]
    base[3]["amount"] = None            # -> cuarentena
    base[5]["amount"] = -50.0           # -> flag NEGATIVE_AMOUNT
    pd.DataFrame(base, columns=COLUMNS).to_parquet(
        raw / "movimientos_2024-09-15.parquet", index=False)

    siguiente = [dict(r) for r in base[:110]]
    for r in siguiente[:15]:
        r["amount"] = (r["amount"] or 0) + 500     # correcciones
    siguiente.append(row(id_cliente="CLI000999", amount=777.0))   # alta
    pd.DataFrame(siguiente, columns=COLUMNS).to_parquet(
        raw / "movimientos_2024-09-16.parquet", index=False)

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    load_config.cache_clear()
    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_OK
    yield tmp_path
    load_config.cache_clear()


def test_el_tablero_se_renderiza_sin_excepciones(poblada):
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    st.cache_resource.clear()
    st.cache_data.clear()

    at = AppTest.from_file(str(APP), default_timeout=180)
    at.run()

    assert not at.exception, [str(e.value) for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert len(at.tabs) == 6, "las seis secciones del tablero deben existir"
    labels = [m.label for m in at.metric]
    assert "Movimientos vigentes" in labels
    assert "Monto vigente" in labels
    assert len(at.dataframe) > 0


def _run_with_db(tmp_path, monkeypatch, db_file: Path):
    """Arranca el tablero apuntando a un archivo de base concreto."""
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from src.config import load_config

    # El tablero mantiene la conexion en cache_resource (deseable en produccion:
    # evita reabrir la base en cada interaccion). En la prueba hay que vaciarla
    # para que no reutilice la base del test anterior.
    st.cache_resource.clear()
    st.cache_data.clear()

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(db_file))
    load_config.cache_clear()

    at = AppTest.from_file(str(APP), default_timeout=60)
    at.run()
    load_config.cache_clear()
    return at


def test_el_tablero_avisa_si_no_hay_base(tmp_path, monkeypatch):
    at = _run_with_db(tmp_path, monkeypatch, tmp_path / "no_existe.duckdb")

    assert not at.exception
    assert at.error, "debe explicar que hay que ejecutar el pipeline primero"
    assert "No existe la base de datos" in at.error[0].value
    assert "docker compose up" in at.error[0].value


def test_el_tablero_avisa_si_la_base_esta_vacia(tmp_path, monkeypatch):
    """Una ejecucion interrumpida deja un archivo de 0 bytes.

    DuckDB lo rechaza con 'not a valid DuckDB database file'. El tablero debe
    explicarlo, no reventar con un traceback.
    """
    vacia = tmp_path / "vacia.duckdb"
    vacia.write_bytes(b"")

    at = _run_with_db(tmp_path, monkeypatch, vacia)

    assert not at.exception, [str(e.value) for e in at.exception]
    assert at.error
    assert "vacio" in at.error[0].value
    assert "docker compose up" in at.error[0].value


def test_el_tablero_avisa_si_la_base_es_invalida(tmp_path, monkeypatch):
    """Un archivo que existe y tiene contenido, pero no es una base DuckDB."""
    invalida = tmp_path / "invalida.duckdb"
    invalida.write_bytes(b"esto no es una base de datos de DuckDB")

    at = _run_with_db(tmp_path, monkeypatch, invalida)

    assert not at.exception, [str(e.value) for e in at.exception]
    assert at.error
    assert "no es una base DuckDB valida" in at.error[0].value


def test_el_tablero_no_borra_la_base_invalida(tmp_path, monkeypatch):
    """El tablero es un consumidor de SOLO LECTURA: no toca los archivos."""
    invalida = tmp_path / "invalida.duckdb"
    contenido = b"contenido que no debe desaparecer"
    invalida.write_bytes(contenido)

    _run_with_db(tmp_path, monkeypatch, invalida)

    assert invalida.exists()
    assert invalida.read_bytes() == contenido
