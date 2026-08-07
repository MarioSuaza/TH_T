"""Pruebas de extremo a extremo del orquestador, incluidos los archivos reales."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from src import pipeline as pipeline_mod
from tests.conftest import COLUMNS, row

REAL_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
REAL_FILES = ["movimientos_dia_T.parquet", "movimientos_dia_T1.parquet"]


# --------------------------------------------------------------- CLI y codigos
def test_el_pipeline_completo_devuelve_cero(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    pd.DataFrame([row(id_cliente=f"CLI0000{i:02d}") for i in range(1, 11)],
                 columns=COLUMNS).to_parquet(raw / "movimientos_2024-09-15.parquet", index=False)
    pd.DataFrame([row(id_cliente=f"CLI0000{i:02d}") for i in range(1, 9)],
                 columns=COLUMNS).to_parquet(raw / "movimientos_2024-09-16.parquet", index=False)

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    from src.config import load_config
    load_config.cache_clear()

    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_OK
    assert (tmp_path / "reports" / "insights.md").exists()
    load_config.cache_clear()


def test_un_directorio_sin_archivos_es_error_critico(tmp_path, monkeypatch):
    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    from src.config import load_config
    load_config.cache_clear()
    vacio = tmp_path / "vacio"
    vacio.mkdir()
    assert pipeline_mod.main(["--input-directory", str(vacio)]) == pipeline_mod.EXIT_CRITICAL
    load_config.cache_clear()


def test_una_columna_obligatoria_faltante_devuelve_codigo_uno(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    cols = [c for c in COLUMNS if c != "amount"]
    pd.DataFrame([{k: v for k, v in row().items() if k != "amount"}],
                 columns=cols).to_parquet(raw / "movimientos_2024-09-15.parquet", index=False)

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    from src.config import load_config
    load_config.cache_clear()
    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_CRITICAL
    load_config.cache_clear()


def test_un_fallo_al_abrir_la_base_devuelve_codigo_uno_sin_escapar(
        tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()

    class LockedDatabase:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("database is locked")

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    monkeypatch.setattr(pipeline_mod, "Database", LockedDatabase)
    from src.config import load_config
    load_config.cache_clear()

    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_CRITICAL
    load_config.cache_clear()


# ------------------------------------------------------------------- guardas
def test_una_caida_masiva_de_volumen_no_aplica_las_bajas(db, cfg, make_parquet, load):
    grande = [row(id_cliente=f"CLI{i:06d}") for i in range(1, 501)]
    load(make_parquet(grande, name="t.parquet"), "2024-09-15")
    activos_antes = db.scalar("SELECT count(*) FROM movements_current WHERE is_active")

    # El corte siguiente llega truncado: solo 10 de las 500 claves.
    r = load(make_parquet(grande[:10], name="t1.parquet"), "2024-09-16")

    assert r.status == "FAILED"
    assert r.exit_code == pipeline_mod.EXIT_GUARD
    assert db.scalar("SELECT count(*) FROM movements_current WHERE is_active") == activos_antes, \
        "un corte sospechoso no debe dar de baja nada"
    assert db.scalar("SELECT status FROM pipeline_runs WHERE run_id = ?",
                     [r.run_id]) == "FAILED_GUARD"
    assert db.scalar("SELECT count(*) FROM run_alerts WHERE severity = 'CRITICAL'") >= 1


def test_la_alerta_de_aviso_no_detiene_la_carga(db, cfg, make_parquet, load):
    grande = [row(id_cliente=f"CLI{i:06d}") for i in range(1, 501)]
    load(make_parquet(grande, name="t.parquet"), "2024-09-15")
    # 15% de bajas: supera el aviso (10%) pero no el fallo (30%).
    r = load(make_parquet(grande[:425], name="t1.parquet"), "2024-09-16")
    assert r.status == "SUCCESS"
    assert db.scalar("SELECT count(*) FROM run_alerts WHERE alert_code = 'HIGH_DELETION_RATE' "
                     "AND severity = 'WARNING'") == 1


def test_un_exceso_de_rechazos_detiene_la_carga(db, cfg, make_parquet, load):
    rows = [row(id_cliente=f"CLI{i:06d}") for i in range(1, 101)]
    for r_ in rows[:50]:
        r_["amount"] = None       # 50% de rechazos, por encima del umbral de fallo
    r = load(make_parquet(rows, name="t.parquet"), "2024-09-15")
    assert r.exit_code == pipeline_mod.EXIT_CRITICAL
    assert db.scalar("SELECT error_code FROM pipeline_runs WHERE run_id = ?",
                     [r.run_id]) == "REJECTION_RATE_EXCEEDED"
    assert db.scalar("SELECT count(*) FROM movements_current") == 0


# -------------------------------------------------------------- observabilidad
def test_cada_ejecucion_registra_sus_metricas(db, cfg, make_parquet, load):
    """pipeline_runs es la unica fuente de metricas por ejecucion (ver ADR-015
    en docs/decisions.md): ya no existe una tabla run_metrics duplicada."""
    r = load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 4)],
                          name="t.parquet"), "2024-09-15")
    run = db.execute(
        "SELECT rows_read, rows_valid, rows_rejected, rows_new, rows_updated, "
        "rows_deleted, rows_unchanged, rows_current_after, amount_current, "
        "duration_seconds FROM pipeline_runs WHERE run_id = ?", [r.run_id]).fetchone()
    assert all(v is not None for v in run), f"faltan metricas en pipeline_runs: {run}"
    assert run[9] > 0, "duration_seconds debe ser positiva"


# ---------------------------------------------------------- datos reales de Tyba
@pytest.mark.realdata
@pytest.mark.skipif(not all((REAL_DIR / f).exists() for f in REAL_FILES),
                    reason="los parquet de Tyba no estan en data/raw")
def test_extremo_a_extremo_con_los_archivos_reales(tmp_path, monkeypatch):
    """Carga T y T+1 reales y verifica las invariantes y la reconciliacion."""
    raw = tmp_path / "raw"
    raw.mkdir()
    for f in REAL_FILES:
        shutil.copy(REAL_DIR / f, raw / f)

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    from src.config import load_config
    load_config.cache_clear()

    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_OK

    import duckdb
    con = duckdb.connect(str(tmp_path / "db.duckdb"), read_only=True)

    runs = con.execute(
        "SELECT snapshot_date, rows_read, rows_valid, rows_rejected, rows_new, "
        "rows_updated, rows_deleted, rows_unchanged, rows_current_after "
        "FROM pipeline_runs WHERE status = 'SUCCESS' ORDER BY snapshot_date").fetchall()
    assert len(runs) == 2

    t, t1 = runs
    assert t[1] == 50_000 and t1[1] == 49_000
    # Conservacion de filas en ambos cortes
    assert t[1] == t[2] + t[3]
    assert t1[1] == t1[2] + t1[3]
    # La clasificacion cubre exactamente las filas validas del corte
    assert t1[4] + t1[5] + t1[7] == t1[2]
    # Continuidad del estado vigente
    assert t[8] + t1[4] - t1[6] == t1[8]
    # El corte T+1 tiene los cuatro casos del enunciado
    assert t1[4] > 0 and t1[5] > 0 and t1[6] > 0 and t1[7] > 0

    # Todas las reconciliaciones cuadran
    assert con.execute(
        "SELECT count(*) FROM reconciliation_results WHERE NOT passed").fetchone()[0] == 0
    # Invariantes estructurales
    assert con.execute("SELECT count(*) FROM (SELECT movement_key FROM movements_current "
                       "GROUP BY 1 HAVING count(*) > 1)").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM (SELECT movement_key FROM movements_history "
                       "WHERE is_current GROUP BY 1 HAVING count(*) > 1)").fetchone()[0] == 0
    # La normalizacion dejo catalogos limpios
    assert {r[0] for r in con.execute("SELECT DISTINCT type FROM movements_current").fetchall()} == {"IN", "OUT"}
    assert con.execute("SELECT count(DISTINCT fund) FROM movements_current").fetchone()[0] == 7
    con.close()
    load_config.cache_clear()


@pytest.mark.realdata
@pytest.mark.skipif(not all((REAL_DIR / f).exists() for f in REAL_FILES),
                    reason="los parquet de Tyba no estan en data/raw")
def test_idempotencia_con_los_archivos_reales(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    for f in REAL_FILES:
        shutil.copy(REAL_DIR / f, raw / f)

    monkeypatch.setenv("TYBA_PATHS__DATABASE_FILE", str(tmp_path / "db.duckdb"))
    monkeypatch.setenv("TYBA_PATHS__REPORTS_DIR", str(tmp_path / "reports"))
    from src.config import load_config
    load_config.cache_clear()

    pipeline_mod.main(["--input-directory", str(raw)])

    import duckdb

    def state():
        con = duckdb.connect(str(tmp_path / "db.duckdb"), read_only=True)
        s = con.execute(
            "SELECT (SELECT count(*) FROM movements_current), "
            "       (SELECT count(*) FROM movements_current WHERE is_active), "
            "       (SELECT count(*) FROM movements_history), "
            "       (SELECT count(*) FROM movement_changes), "
            "       (SELECT sum(amount) FROM movements_current WHERE is_active), "
            "       (SELECT count(*) FROM pipeline_runs WHERE status='SUCCESS')").fetchone()
        con.close()
        return s

    before = state()
    assert pipeline_mod.main(["--input-directory", str(raw)]) == pipeline_mod.EXIT_OK
    assert state() == before, "reprocesar los mismos archivos no puede alterar nada"
    load_config.cache_clear()
