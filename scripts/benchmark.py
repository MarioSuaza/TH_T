#!/usr/bin/env python3
"""Prueba de escala con datos SINTETICOS.

    python scripts/benchmark.py --rows 100000 1000000

Genera dos cortes sinteticos por tamano (T y T+1 con un 20% de bajas, un 8% de
correcciones y un 18% de altas), ejecuta el pipeline completo sobre una base
temporal y mide tiempo, memoria y tamano en disco.

IMPORTANTE: los resultados son de datos generados, no de los archivos de Tyba.
No deben mezclarse con las cifras reales. La salida va a un archivo distinto
(`benchmark_synthetic.csv` / `.md`) precisamente para evitar esa confusion.
"""

from __future__ import annotations

import argparse
import gc
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.logging_config import get_logger, setup_logging  # noqa: E402

log = get_logger("benchmark")

PRODUCTS = ["Acciones", "Bonos", "CDT", "Cuenta de Ahorro", "Divisas", "ETF",
            "Fondo de Inversión", "Fondo de Pensión"]
FUNDS = ["Balanceado", "Conservador", "Crecimiento", "Internacional",
         "Mercado Monetario", "Renta Fija", "Renta Variable"]
# Se reproducen las mismas suciedades observadas en los archivos reales para que
# la medicion incluya el coste de normalizarlas.
TYPES = ["entrada", "Entrada", "ENTRADA", "in", "IN", "salida", "Salida", "SALIDA", "out", "OUT"]
DESCRIPTIONS = ["Deposito inicial", "Retiro parcial", "Compra de activo",
                "Venta de activo", "Comision de administracion", None]
COMMERCIALS = ["Bancolombia", "Davivienda", "BBVA", "Itau", "Scotiabank", None]

COLUMNS = ["id_cliente", "date", "product", "type", "fund", "amount",
           "description", "commercial_name"]


def synth(n: int, seed: int, start_id: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-09-15", periods=60).strftime("%Y-%m-%d").to_numpy()
    dmy = pd.date_range("2024-09-15", periods=60).strftime("%d/%m/%Y").to_numpy()
    use_dmy = rng.random(n) < 0.07          # ~7% en el formato alternativo
    idx = rng.integers(0, len(dates), n)

    amounts = np.round(rng.uniform(-5e7, 5e7, n), 2)
    amounts[rng.random(n) < 0.03] = np.nan  # ~3% de montos nulos

    funds = rng.choice(FUNDS, n).astype(object)
    dirty = rng.random(n) < 0.06
    funds[dirty] = np.char.add("  ", np.char.upper(funds[dirty].astype(str)))

    return pd.DataFrame({
        "id_cliente": [f"CLI{i:07d}" for i in
                       rng.integers(start_id, start_id + max(n // 15, 1), n)],
        "date": np.where(use_dmy, dmy[idx], dates[idx]),
        "product": rng.choice(PRODUCTS, n),
        "type": rng.choice(TYPES, n),
        "fund": funds,
        "amount": amounts,
        "description": rng.choice(DESCRIPTIONS, n),
        "commercial_name": rng.choice(COMMERCIALS, n),
    }, columns=COLUMNS)


def evolve(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Deriva T+1: ~20% de bajas, ~8% de correcciones y ~18% de altas."""
    rng = np.random.default_rng(seed + 1)
    kept = df.sample(frac=0.80, random_state=seed).copy()
    corrected = kept.sample(frac=0.10, random_state=seed + 2).index
    kept.loc[corrected, "amount"] = np.round(
        rng.uniform(-5e7, 5e7, len(corrected)), 2)
    nuevos = synth(int(len(df) * 0.18), seed + 3, start_id=9_000_000)
    return pd.concat([kept, nuevos], ignore_index=True)


def _rss_to_mb(value: int) -> float:
    # ru_maxrss viene en KiB en Linux y en bytes en macOS.
    return value / (1024 if sys.platform != "darwin" else 1024 * 1024)


def child_peak_memory_mb() -> float:
    """Pico de memoria del proceso HIJO que ejecuto el pipeline.

    El pipeline se lanza en un subproceso a proposito: si se midiera en este
    mismo proceso, el pico incluiria la memoria del generador de datos
    sinteticos (numpy y pandas con millones de filas) y la cifra no describiria
    el pipeline.
    """
    return _rss_to_mb(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)


def run_case(rows: int, workdir: Path) -> dict:
    case = workdir / f"case_{rows}"
    raw = case / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    t_gen = time.perf_counter()
    t0 = synth(rows, seed=rows)
    t1 = evolve(t0, seed=rows)
    expected_rows_total = len(t0) + len(t1)
    t0.to_parquet(raw / "movimientos_2030-01-01.parquet", index=False)
    t1.to_parquet(raw / "movimientos_2030-01-02.parquet", index=False)
    gen_seconds = time.perf_counter() - t_gen
    # Liberar antes de medir: si no, el pico incluiria el del generador.
    del t0, t1
    gc.collect()

    db_path = case / "bench.duckdb"
    # Los datos sinteticos no son plausibles como negocio (montos uniformes en
    # torno a cero): aqui se mide rendimiento, no calidad.
    os.environ["TYBA_GUARDS__ENABLED"] = "false"
    # Techo de memoria: BENCH_MEMORY_LIMIT > el valor ya inyectado por el
    # entorno > 1GB. No pisar el que fija docker-compose.yml.
    if "BENCH_MEMORY_LIMIT" in os.environ:
        os.environ["TYBA_DATABASE__MEMORY_LIMIT"] = os.environ["BENCH_MEMORY_LIMIT"]
    else:
        os.environ.setdefault("TYBA_DATABASE__MEMORY_LIMIT", "1GB")
    os.environ["TYBA_PATHS__DATABASE_FILE"] = str(db_path)
    os.environ["TYBA_PATHS__REPORTS_DIR"] = str(case / "reports")

    rss_before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    started = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-m", "src.pipeline",
         "--input-directory", str(raw), "--skip-analytics", "--log-level", "WARNING"],
        cwd=str(Path(__file__).resolve().parent.parent),
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent.parent)},
        capture_output=True, text=True)
    elapsed = time.perf_counter() - started
    code = proc.returncode
    if code != 0:
        print(proc.stdout[-2000:], proc.stderr[-2000:])
    peak_rss = max(_rss_to_mb(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss),
                   _rss_to_mb(rss_before))

    import duckdb
    con = duckdb.connect(str(db_path), read_only=True)
    runs = con.execute(
        "SELECT sum(rows_read), sum(rows_valid), sum(rows_rejected), sum(rows_new), "
        "sum(rows_updated), sum(rows_deleted), sum(rows_unchanged), sum(duration_seconds) "
        "FROM pipeline_runs WHERE status = 'SUCCESS'").fetchone()
    run_status = con.execute(
        "SELECT count(*) FILTER (WHERE status = 'SUCCESS'), "
        "       count(*) FILTER (WHERE status <> 'SUCCESS') "
        "FROM pipeline_runs").fetchone()
    current = con.execute("SELECT count(*) FROM movements_current").fetchone()[0]
    history = con.execute("SELECT count(*) FROM movements_history").fetchone()[0]
    con.close()

    parquet_bytes = sum(f.stat().st_size for f in raw.glob("*.parquet"))
    db_bytes = db_path.stat().st_size if db_path.exists() else 0
    total_rows = int(runs[0] or 0)
    success_runs = int(run_status[0] or 0)
    failed_runs = int(run_status[1] or 0)
    benchmark_passed = (
        code == 0
        and success_runs == 2
        and failed_runs == 0
        and total_rows == expected_rows_total
    )

    return {
        "rows_per_snapshot": rows,
        "rows_expected_total": expected_rows_total,
        "rows_processed_total": total_rows,
        "exit_code": code,
        "successful_runs": success_runs,
        "failed_runs": failed_runs,
        "benchmark_passed": benchmark_passed,
        "generation_seconds": round(gen_seconds, 2),
        "pipeline_seconds": round(elapsed, 2),
        "seconds_per_snapshot": round(
            float(runs[7] or 0) / success_runs, 3) if success_runs else 0.0,
        "rows_per_second": int(total_rows / elapsed) if elapsed else 0,
        "rows_valid": int(runs[1] or 0),
        "rows_rejected": int(runs[2] or 0),
        "rows_new": int(runs[3] or 0),
        "rows_updated": int(runs[4] or 0),
        "rows_deleted": int(runs[5] or 0),
        "rows_unchanged": int(runs[6] or 0),
        "movements_current": current,
        "movements_history": history,
        "parquet_input_mb": round(parquet_bytes / 1e6, 2),
        "duckdb_size_mb": round(db_bytes / 1e6, 2),
        "peak_rss_mb": round(peak_rss, 1),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Prueba de escala con datos sinteticos")
    ap.add_argument("--rows", type=int, nargs="+", default=[50_000, 250_000, 1_000_000])
    ap.add_argument("--keep", action="store_true", help="conserva los archivos generados")
    ap.add_argument("--output-dir", type=Path, default=None)
    args = ap.parse_args(argv)

    setup_logging("WARNING")
    workdir = Path(tempfile.mkdtemp(prefix="tyba_bench_"))
    results = []
    try:
        for rows in sorted(args.rows):
            print(f"→ {rows:,} filas por corte…", flush=True)
            r = run_case(rows, workdir)
            results.append(r)
            # Con 1M de filas la base supera 1 GB: no acumular casos.
            if not args.keep:
                shutil.rmtree(workdir / f"case_{rows}", ignore_errors=True)
            print(f"  {r['pipeline_seconds']}s | {r['rows_per_second']:,} filas/s | "
                  f"pico RSS {r['peak_rss_mb']} MB | base {r['duckdb_size_mb']} MB",
                  flush=True)
            if not r["benchmark_passed"]:
                print("  FALLO: el benchmark no completo y valido sus dos cortes "
                      f"(exit={r['exit_code']}, success={r['successful_runs']}, "
                      f"failed={r['failed_runs']}, filas="
                      f"{r['rows_processed_total']:,}/{r['rows_expected_total']:,}).",
                      flush=True)
    finally:
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)
        else:
            print(f"Casos conservados en {workdir}", flush=True)

    df = pd.DataFrame(results)
    out = args.output_dir or (Path(__file__).resolve().parent.parent / "data" / "reports")
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "benchmark_synthetic.csv", index=False)

    L = ["# Prueba de escala (datos sinteticos)", "",
         "> **Estos numeros NO describen los archivos de Tyba.** Se generan con "
         "`scripts/benchmark.py` sobre datos sinteticos que reproducen las mismas "
         "suciedades observadas (dos formatos de fecha, diez variantes de `type`, "
         "fondos con mayusculas y espacios, ~3% de montos nulos) y una evolucion "
         "de ~20% de bajas, ~8% de correcciones y ~18% de altas entre cortes.", "",
         f"Entorno: Python {sys.version.split()[0]}, "
         f"{os.cpu_count()} CPU logicas.", "",
         "| Estado | Exit | Filas/corte | Total procesado | Tiempo pipeline | Filas/s | Pico RSS | Parquet entrada | Base DuckDB | Vigentes | Historico |",
         "| :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in results:
        L.append(
            f"| {'OK' if r['benchmark_passed'] else 'FALLO'} | {r['exit_code']} | "
            f"{r['rows_per_snapshot']:,} | {r['rows_processed_total']:,} | "
            f"{r['pipeline_seconds']:.2f}s | {r['rows_per_second']:,} | "
            f"{r['peak_rss_mb']} MB | {r['parquet_input_mb']} MB | "
            f"{r['duckdb_size_mb']} MB | {r['movements_current']:,} | "
            f"{r['movements_history']:,} |")
    L += ["", "## Clasificacion de cambios obtenida", "",
          "| Filas/corte | NEW | UPDATED | DELETED | UNCHANGED | Rechazadas |",
          "| ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in results:
        L.append(f"| {r['rows_per_snapshot']:,} | {r['rows_new']:,} | {r['rows_updated']:,} | "
                 f"{r['rows_deleted']:,} | {r['rows_unchanged']:,} | {r['rows_rejected']:,} |")
    (out / "benchmark_synthetic.md").write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"\nResultados en {out / 'benchmark_synthetic.md'}")
    return 0 if all(r["benchmark_passed"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
