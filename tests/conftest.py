"""Fixtures compartidas.

Todas las pruebas trabajan con datos sinteticos pequenos y controlados
(`make_parquet`). Los archivos reales de Tyba solo se usan en la prueba
end-to-end marcada como `realdata`, que puede desactivarse con
`pytest -m "not realdata"`.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import load_config  # noqa: E402
from src.database import Database  # noqa: E402
from src.logging_config import setup_logging  # noqa: E402

COLUMNS = ["id_cliente", "date", "product", "type", "fund", "amount",
           "description", "commercial_name"]


@pytest.fixture(scope="session", autouse=True)
def _quiet_logging():
    setup_logging("WARNING")


@pytest.fixture()
def cfg(tmp_path, monkeypatch):
    """Configuracion real del repositorio, con la base y los reportes en tmp."""
    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "test.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    load_config.cache_clear()
    c = load_config()
    c.paths.ensure()
    yield c
    load_config.cache_clear()


@pytest.fixture()
def db(cfg):
    d = Database(cfg, ":memory:")
    d.init_schema()
    yield d
    d.close()


def row(id_cliente="CLI000001", d="2024-09-15", product="Bonos", type_="entrada",
        fund="Renta Fija", amount=100.00, description="Deposito inicial",
        commercial_name="Bancolombia") -> dict:
    """Una fila valida por defecto; se sobreescribe lo que interese."""
    return {"id_cliente": id_cliente, "date": d, "product": product, "type": type_,
            "fund": fund, "amount": amount, "description": description,
            "commercial_name": commercial_name}


@pytest.fixture()
def make_parquet(tmp_path):
    """Crea un parquet con las filas dadas y devuelve su ruta."""
    counter = {"n": 0}

    def _make(rows: list[dict], name: str | None = None,
              columns: list[str] | None = None) -> Path:
        counter["n"] += 1
        filename = name or f"snapshot_{counter['n']}.parquet"
        path = tmp_path / filename
        df = pd.DataFrame(rows, columns=columns or COLUMNS)
        df.to_parquet(path, index=False)
        return path

    return _make


@pytest.fixture()
def load(db, cfg):
    """Ejecuta una carga completa de un archivo y devuelve el resultado."""
    from src.ingestion import read_metadata, resolve_snapshot_date, validate_schema
    from src.pipeline import process_snapshot

    def _load(path: Path, snapshot_date: str, force: bool = False):
        meta = read_metadata(path, cfg)
        validate_schema(meta, cfg)
        resolve_snapshot_date(meta, cfg, date.fromisoformat(snapshot_date))
        return process_snapshot(db, cfg, meta, force=force)

    return _load
