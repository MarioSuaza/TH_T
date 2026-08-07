"""Trazabilidad de la version exacta del contrato aplicada en cada run."""

from __future__ import annotations

import hashlib

from tests.conftest import ROOT, row


def test_config_expone_version_y_hash_exacto_del_contrato(cfg):
    contract_path = ROOT / "config" / "data_contract.yml"

    assert cfg.contract_path == contract_path
    assert cfg.contract_version == "1.0.0"
    assert cfg.contract_hash == hashlib.sha256(contract_path.read_bytes()).hexdigest()
    assert len(cfg.contract_hash) == 64


def test_cada_ejecucion_persiste_el_contrato_utilizado(db, cfg, make_parquet, load):
    result = load(make_parquet([row()], name="t.parquet"), "2024-09-15")

    persisted = db.execute(
        "SELECT contract_version, contract_hash FROM pipeline_runs WHERE run_id = ?",
        [result.run_id],
    ).fetchone()

    assert persisted == (cfg.contract_version, cfg.contract_hash)


def test_la_migracion_agrega_columnas_a_una_base_existente(db):
    db.execute("ALTER TABLE pipeline_runs DROP COLUMN contract_version")
    db.execute("ALTER TABLE pipeline_runs DROP COLUMN contract_hash")

    db.init_schema()
    db.init_schema()  # tambien debe ser segura al repetirse

    columns = {
        row_[0]
        for row_ in db.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = 'pipeline_runs'"
        ).fetchall()
    }
    assert {"contract_version", "contract_hash"} <= columns
