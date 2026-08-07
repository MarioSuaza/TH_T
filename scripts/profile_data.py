#!/usr/bin/env python3
"""Perfilado de los archivos parquet de entrada.

Se ejecuta ANTES de decidir nada sobre el pipeline y es independiente de el: no
escribe en la base ni modifica los archivos. Su salida son los reportes que
sustentan las decisiones documentadas en docs/analysis_and_assumptions.md.

    python scripts/profile_data.py                 # perfila data/raw completo
    python scripts/profile_data.py --input-dir X   # otro directorio

Genera en data/reports/:
    data_profile_summary.csv    una fila por archivo
    data_quality_summary.csv    un control de calidad por fila
    schema_comparison.csv       esquema recibido vs contrato
    duplicate_analysis.csv      duplicados exactos y claves candidatas
    column_profile.csv          perfil por columna
    categorical_values.csv      valores distintos de las columnas categoricas
    snapshot_comparison.csv     comparacion entre cortes consecutivos
    data_profile.md             informe legible
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config  # noqa: E402
from src.ingestion import read_metadata, sha256_file  # noqa: E402
from src.logging_config import get_logger, setup_logging  # noqa: E402
from src.normalization import (date_expression, fund_expression,  # noqa: E402
                               product_expression, type_expression)

log = get_logger("profile")

CATEGORICAL = ["product", "type", "fund", "description", "commercial_name"]


def _norm_view(cfg, path: Path) -> str:
    return f"""
    SELECT
        CAST(id_cliente AS VARCHAR) AS id_cliente_raw,
        CAST(date AS VARCHAR)       AS date_raw,
        CAST(product AS VARCHAR)    AS product_raw,
        CAST(type AS VARCHAR)       AS type_raw,
        CAST(fund AS VARCHAR)       AS fund_raw,
        CAST(amount AS VARCHAR)     AS amount_text,
        TRY_CAST(amount AS DOUBLE)  AS amount_raw,
        TRY_CAST(amount AS {cfg.amount_sql_type}) AS amount_decimal,
        CAST(description AS VARCHAR)     AS description_raw,
        CAST(commercial_name AS VARCHAR) AS commercial_name_raw,
        {date_expression(cfg)}    AS movement_date,
        {product_expression(cfg)} AS product_norm,
        {type_expression(cfg)}    AS type_norm,
        {fund_expression(cfg)}    AS fund_norm
    FROM read_parquet('{path}')
    """


def profile(cfg, files: list[Path]) -> dict[str, pd.DataFrame]:
    con = duckdb.connect()
    out: dict[str, list] = {k: [] for k in
                            ("summary", "quality", "schema", "duplicates",
                             "columns", "categoricals")}

    for path in files:
        meta = read_metadata(path, cfg)
        con.execute(f"CREATE OR REPLACE VIEW v AS {_norm_view(cfg, path)}")
        name = path.name
        n = meta.row_count

        # ---------------------------------------------------------- resumen
        row = con.execute("""
            SELECT
                count(*)                                                   AS rows,
                count(DISTINCT id_cliente_raw)                             AS distinct_id_cliente,
                count(*) FILTER (WHERE id_cliente_raw IS NULL)             AS null_id,
                count(*) FILTER (WHERE trim(coalesce(id_cliente_raw,'')) = '') AS empty_id,
                count(*) FILTER (WHERE movement_date IS NULL)              AS unparseable_date,
                min(movement_date)                                         AS date_min,
                max(movement_date)                                         AS date_max,
                count(DISTINCT movement_date)                              AS distinct_dates,
                count(*) FILTER (WHERE amount_text IS NULL)                AS null_amount,
                count(*) FILTER (WHERE amount_text IS NOT NULL AND amount_raw IS NULL)
                                                                            AS invalid_amount_format,
                count(*) FILTER (WHERE amount_raw IS NOT NULL
                                  AND NOT isnan(amount_raw) AND NOT isinf(amount_raw)
                                  AND amount_decimal IS NULL)                AS amount_out_of_range,
                count(*) FILTER (WHERE isnan(amount_raw) OR isinf(amount_raw)) AS non_finite_amount,
                count(*) FILTER (WHERE amount_raw < 0)                     AS negative_amount,
                count(*) FILTER (WHERE amount_raw = 0)                     AS zero_amount,
                min(amount_raw)                                            AS amount_min,
                max(amount_raw)                                            AS amount_max,
                avg(amount_raw)                                            AS amount_avg,
                count(*) FILTER (WHERE type_norm IS NULL)                  AS unmappable_type,
                count(DISTINCT type_raw)                                   AS distinct_type_raw,
                count(DISTINCT fund_raw)                                   AS distinct_fund_raw,
                count(DISTINCT fund_norm)                                  AS distinct_fund_norm,
                count(*) FILTER (WHERE fund_raw <> trim(fund_raw))         AS fund_with_outer_spaces,
                count(*) FILTER (WHERE regexp_matches(fund_raw, '\\s\\s'))  AS fund_with_double_spaces
            FROM v""").df().iloc[0].to_dict()
        row.update({"file": name, "path": str(path), "size_bytes": meta.size_bytes,
                    "sha256": meta.sha256, "columns": len(meta.column_names),
                    "column_names": "|".join(meta.column_names)})
        out["summary"].append(row)

        # -------------------------------------------------------- esquema
        for col, actual in meta.column_types.items():
            declared = next((c for c in cfg.columns if c["name"] == col), None)
            out["schema"].append({
                "file": name, "column": col, "actual_type": actual,
                "contract_type": declared["physical_type"] if declared else "(no declarada)",
                "in_contract": declared is not None,
                "required": bool(declared and declared.get("required")),
            })
        for missing in set(cfg.required_columns) - set(meta.column_names):
            out["schema"].append({"file": name, "column": missing, "actual_type": "(ausente)",
                                  "contract_type": cfg.column(missing)["physical_type"],
                                  "in_contract": True, "required": True})

        # ------------------------------------------------------ duplicados
        dup = con.execute("""
            SELECT
                (SELECT count(*) FROM (SELECT id_cliente_raw, date_raw, product_raw, type_raw,
                                              fund_raw, amount_raw, description_raw, commercial_name_raw
                                       FROM v GROUP BY ALL HAVING count(*) > 1)) AS exact_dup_groups,
                (SELECT count(*) FROM (SELECT id_cliente_raw FROM v GROUP BY 1 HAVING count(*) > 1)) AS id_cliente_repeated,
                count(DISTINCT (id_cliente_raw, movement_date))                                    AS k_id_date,
                count(DISTINCT (id_cliente_raw, movement_date, product_norm))                      AS k_id_date_prod,
                count(DISTINCT (id_cliente_raw, movement_date, product_norm, fund_norm))           AS k4,
                count(DISTINCT (id_cliente_raw, movement_date, product_norm, fund_norm, type_norm)) AS k5,
                count(*)                                                                            AS rows
            FROM v""").df().iloc[0].to_dict()
        dup["file"] = name
        dup["k5_uniqueness"] = dup["k5"] / dup["rows"] if dup["rows"] else 0
        dup["k5_collision_rows"] = con.execute("""
            SELECT coalesce(sum(c), 0) FROM (
              SELECT count(*) AS c FROM v
              GROUP BY id_cliente_raw, movement_date, product_norm, fund_norm, type_norm
              HAVING count(*) > 1)""").fetchone()[0]
        out["duplicates"].append(dup)

        # --------------------------------------------------- perfil columna
        for col in meta.column_names:
            raw = f"{col}_raw" if f"{col}_raw" in ("id_cliente_raw", "date_raw", "product_raw",
                                                   "type_raw", "fund_raw", "amount_raw",
                                                   "description_raw", "commercial_name_raw") else col
            stats = con.execute(f"""
                SELECT count(*) FILTER (WHERE {raw} IS NULL) AS nulls,
                       count(DISTINCT {raw})                 AS distinct_values
                FROM v""").df().iloc[0]
            out["columns"].append({
                "file": name, "column": col,
                "physical_type": meta.column_types[col],
                "nulls": int(stats.nulls),
                "null_pct": float(stats.nulls) / n if n else 0,
                "distinct_values": int(stats.distinct_values),
            })

        # ------------------------------------------------- categoricos
        for col in CATEGORICAL:
            df = con.execute(
                f"SELECT CAST({col}_raw AS VARCHAR) AS value, count(*) AS rows "
                f"FROM v GROUP BY 1 ORDER BY 2 DESC LIMIT 60").df()
            df["file"], df["column"] = name, col
            out["categoricals"].append(df)

        log.info("Perfilado %s: %s filas, %s columnas", name, f"{n:,}", len(meta.column_names))

    frames = {
        "summary": pd.DataFrame(out["summary"]),
        "schema": pd.DataFrame(out["schema"]),
        "duplicates": pd.DataFrame(out["duplicates"]),
        "columns": pd.DataFrame(out["columns"]),
        "categoricals": pd.concat(out["categoricals"], ignore_index=True) if out["categoricals"] else pd.DataFrame(),
    }

    # ------------------------------------------------- controles de calidad
    checks = []
    for _, r in frames["summary"].iterrows():
        n = int(r["rows"])
        def add(check, observed, severity, note):
            checks.append({"file": r["file"], "check": check, "observed": observed,
                           "pct_of_rows": (observed / n) if n else 0,
                           "severity": severity, "note": note})
        add("filas", n, "INFO", "total de filas del archivo")
        add("id_cliente nulos", int(r["null_id"]), "CRITICAL" if r["null_id"] else "OK", "")
        add("id_cliente vacios", int(r["empty_id"]), "CRITICAL" if r["empty_id"] else "OK", "")
        add("id_cliente distintos", int(r["distinct_id_cliente"]), "INFO",
            "muy inferior al numero de filas: no es clave de transaccion")
        add("fechas no parseables", int(r["unparseable_date"]),
            "CRITICAL" if r["unparseable_date"] else "OK", "con los formatos del contrato")
        add("montos nulos", int(r["null_amount"]),
            "RECORD_ERROR" if r["null_amount"] else "OK", "van a cuarentena")
        add("montos con formato invalido", int(r["invalid_amount_format"]),
            "RECORD_ERROR" if r["invalid_amount_format"] else "OK", "van a cuarentena")
        add("montos fuera de DECIMAL(20,2)", int(r["amount_out_of_range"]),
            "RECORD_ERROR" if r["amount_out_of_range"] else "OK", "van a cuarentena")
        add("montos no finitos", int(r["non_finite_amount"]),
            "RECORD_ERROR" if r["non_finite_amount"] else "OK", "")
        add("montos negativos", int(r["negative_amount"]), "INFO",
            "el enunciado no define el signo: se reporta, no se corrige")
        add("montos cero", int(r["zero_amount"]), "INFO", "")
        add("type no mapeables a IN/OUT", int(r["unmappable_type"]),
            "RECORD_ERROR" if r["unmappable_type"] else "OK", "")
        add("variantes textuales de type", int(r["distinct_type_raw"]), "WARNING",
            "se canonicalizan a IN/OUT")
        add("variantes textuales de fund", int(r["distinct_fund_raw"]), "WARNING",
            f"se canonicalizan a {int(r['distinct_fund_norm'])} valores")
        add("fund con espacios externos", int(r["fund_with_outer_spaces"]), "WARNING", "")
        add("fund con dobles espacios", int(r["fund_with_double_spaces"]), "WARNING", "")
    frames["quality"] = pd.DataFrame(checks)
    return frames


def compare_snapshots(cfg, files: list[Path]) -> pd.DataFrame:
    """Compara cortes consecutivos usando la MISMA clave de negocio del pipeline."""
    if len(files) < 2:
        return pd.DataFrame()
    con = duckdb.connect()
    rows = []
    for a, b in zip(files, files[1:]):
        con.execute(f"CREATE OR REPLACE VIEW va AS {_norm_view(cfg, a)}")
        con.execute(f"CREATE OR REPLACE VIEW vb AS {_norm_view(cfg, b)}")
        r = con.execute("""
            WITH ka AS (SELECT DISTINCT id_cliente_raw AS i, movement_date AS d,
                               product_norm AS p, type_norm AS t, fund_norm AS f FROM va),
                 kb AS (SELECT DISTINCT id_cliente_raw AS i, movement_date AS d,
                               product_norm AS p, type_norm AS t, fund_norm AS f FROM vb)
            SELECT
              (SELECT count(*) FROM va)                                  AS rows_a,
              (SELECT count(*) FROM vb)                                  AS rows_b,
              (SELECT count(*) FROM ka)                                  AS keys_a,
              (SELECT count(*) FROM kb)                                  AS keys_b,
              (SELECT count(*) FROM ka SEMI JOIN kb USING (i,d,p,t,f))   AS keys_both,
              (SELECT count(*) FROM ka ANTI JOIN kb USING (i,d,p,t,f))   AS keys_only_a,
              (SELECT count(*) FROM kb ANTI JOIN ka USING (i,d,p,t,f))   AS keys_only_b
        """).df().iloc[0].to_dict()
        r.update({"snapshot_a": a.name, "snapshot_b": b.name})
        rows.append(r)
    return pd.DataFrame(rows)


def write_markdown(cfg, frames: dict, comparison: pd.DataFrame, out_dir: Path) -> Path:
    s = frames["summary"]
    d = frames["duplicates"]
    L = ["# Perfil de los datos de entrada", "",
         "Generado por `scripts/profile_data.py`. No modifica ningun archivo.", "",
         "## Archivos", "",
         "| Archivo | Filas | Columnas | Tamano | SHA-256 |",
         "| --- | ---: | ---: | ---: | --- |"]
    for _, r in s.iterrows():
        L.append(f"| `{r['file']}` | {int(r['rows']):,} | {int(r['columns'])} | "
                 f"{int(r['size_bytes']):,} B | `{r['sha256'][:16]}…` |")

    L += ["", "## Hallazgo principal: no hay identificador de transaccion", "",
          "| Archivo | Filas | `id_cliente` distintos | Filas por valor |",
          "| --- | ---: | ---: | ---: |"]
    for _, r in s.iterrows():
        ratio = int(r["rows"]) / int(r["distinct_id_cliente"]) if r["distinct_id_cliente"] else 0
        L.append(f"| `{r['file']}` | {int(r['rows']):,} | {int(r['distinct_id_cliente']):,} | {ratio:.1f} |")
    L += ["", "El glosario del enunciado documenta una columna `id` como identificador de "
          "la transaccion. Esa columna no existe en los archivos: la que hay es "
          "`id_cliente`, y se repite decenas de veces. Cualquier comparacion entre cortes "
          "necesita, por tanto, una clave de negocio derivada.", ""]

    L += ["## Unicidad de las claves candidatas", "",
          "| Archivo | Filas | id+fecha | +producto | +fondo | +tipo | Unicidad | Filas en colision |",
          "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for _, r in d.iterrows():
        L.append(f"| `{r['file']}` | {int(r['rows']):,} | {int(r['k_id_date']):,} | "
                 f"{int(r['k_id_date_prod']):,} | {int(r['k4']):,} | {int(r['k5']):,} | "
                 f"{r['k5_uniqueness']:.4%} | {int(r['k5_collision_rows']):,} |")
    L += ["", "La combinacion `id_cliente + fecha + producto + fondo + tipo` (normalizada) "
          "no llega al 100% de unicidad: por eso el pipeline anade un ordinal de "
          "ocurrencia y marca esas filas con `is_key_ambiguous`.", ""]

    L += ["## Calidad por archivo", "",
          "| Archivo | Control | Observado | % filas | Severidad | Nota |",
          "| --- | --- | ---: | ---: | --- | --- |"]
    for _, r in frames["quality"].iterrows():
        L.append(f"| `{r['file']}` | {r['check']} | {int(r['observed']):,} | "
                 f"{r['pct_of_rows']:.2%} | {r['severity']} | {r['note']} |")

    cats = frames["categoricals"]
    for col in ("type", "fund"):
        sub = cats[cats["column"] == col]
        if len(sub):
            L += ["", f"## Variantes textuales de `{col}`", "",
                  "| Archivo | Valor | Filas |", "| --- | --- | ---: |"]
            for _, r in sub.iterrows():
                L.append(f"| `{r['file']}` | `{r['value']}` | {int(r['rows']):,} |")

    if len(comparison):
        L += ["", "## Comparacion entre cortes consecutivos",
              "", "Usando la misma clave de negocio que el pipeline.", "",
              "| Corte A | Corte B | Filas A | Filas B | Claves solo en A | Claves solo en B | Claves en ambos |",
              "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
        for _, r in comparison.iterrows():
            L.append(f"| `{r['snapshot_a']}` | `{r['snapshot_b']}` | {int(r['rows_a']):,} | "
                     f"{int(r['rows_b']):,} | {int(r['keys_only_a']):,} | "
                     f"{int(r['keys_only_b']):,} | {int(r['keys_both']):,} |")

    path = out_dir / "data_profile.md"
    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    return path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Perfila los parquet de entrada")
    ap.add_argument("--input-dir", type=Path, default=None)
    ap.add_argument("--config", type=str, default=None)
    args = ap.parse_args(argv)

    cfg = load_config(args.config)
    setup_logging(cfg.get("logging.level", "INFO"))
    out_dir = cfg.paths.reports
    out_dir.mkdir(parents=True, exist_ok=True)

    base = args.input_dir or cfg.paths.raw
    files = sorted(Path(base).glob("*.parquet"))
    if not files:
        log.error("No hay archivos .parquet en %s", base)
        return 1

    frames = profile(cfg, files)
    comparison = compare_snapshots(cfg, files)

    frames["summary"].to_csv(out_dir / "data_profile_summary.csv", index=False)
    frames["quality"].to_csv(out_dir / "data_quality_summary.csv", index=False)
    frames["schema"].to_csv(out_dir / "schema_comparison.csv", index=False)
    frames["duplicates"].to_csv(out_dir / "duplicate_analysis.csv", index=False)
    frames["columns"].to_csv(out_dir / "column_profile.csv", index=False)
    frames["categoricals"].to_csv(out_dir / "categorical_values.csv", index=False)
    if len(comparison):
        comparison.to_csv(out_dir / "snapshot_comparison.csv", index=False)
    md = write_markdown(cfg, frames, comparison, out_dir)

    log.info("Perfilado completo. Informe: %s", md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
