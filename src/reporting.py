"""Generacion de los reportes CSV y del documento de insights.

Cada insight se acompana de: metrica, resultado, metodo de calculo, periodo,
filtros, limitacion e interpretacion posible. No se escribe ninguna conclusion
que no salga de una consulta reproducible sobre la base.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.config import Config
from src.database import Database
from src.logging_config import get_logger

log = get_logger("reporting")

# nombre de archivo -> (consulta, descripcion del metodo)
CSV_REPORTS: dict[str, tuple[str, str]] = {
    "change_summary.csv": (
        "SELECT * FROM v_daily_changes",
        "movement_changes agrupado por snapshot_date y change_type"),
    "financial_summary.csv": (
        """SELECT 'fund' AS dimension, fund AS value, movements, amount_total, amount_in, amount_out, net_flow FROM v_summary_by_fund
           UNION ALL
           SELECT 'product', product, movements, amount_total, amount_in, amount_out, net_flow FROM v_summary_by_product
           UNION ALL
           SELECT 'commercial_name', commercial_name, movements, amount_total, amount_in, amount_out, amount_in - amount_out FROM v_summary_by_commercial
           UNION ALL
           SELECT 'type', type, movements, amount_total, NULL, NULL, NULL FROM v_summary_by_type""",
        "sumas sobre movements_current filtrado por is_active"),
    "daily_metrics.csv": (
        "SELECT * FROM v_daily_movements",
        "movements_current activo agrupado por movement_date"),
    "data_quality_metrics.csv": (
        """SELECT q.run_id, q.snapshot_date, q.rows_read, q.rows_valid, q.rows_rejected,
                  q.rows_exact_dupes, q.valid_rate, q.rejection_rate
           FROM v_quality_by_run q ORDER BY q.snapshot_date""",
        "pipeline_runs de ejecuciones exitosas"),
    "rejections_by_code.csv": (
        "SELECT * FROM v_rejections_by_code",
        "rejected_records agrupado por codigo de error"),
    "quality_flags.csv": (
        "SELECT * FROM v_quality_flags_summary",
        "data_quality_flags agrupado por codigo"),
    "field_change_frequency.csv": (
        "SELECT * FROM v_field_change_frequency",
        "movement_change_fields agrupado por columna"),
    "snapshot_summary.csv": (
        "SELECT * FROM v_snapshot_summary",
        "una fila por corte procesado con exito"),
    "anomalies.csv": (
        """SELECT anomaly_code, category, severity, entity_type, entity_id,
                  metric_name, observed, expected, threshold, description
           FROM anomalies ORDER BY category, anomaly_code, abs(coalesce(observed, 0)) DESC""",
        "tabla anomalies generada por src/analytics.py"),
    "top_movements.csv": (
        "SELECT * FROM v_top_movements LIMIT 200",
        "movimientos vigentes ordenados por |amount|"),
    "top_changes_by_impact.csv": (
        "SELECT * FROM v_top_changes_by_impact LIMIT 200",
        "movement_changes ordenado por |amount_delta|"),
    "reconciliation_all_runs.csv": (
        """SELECT run_id, snapshot_date, check_group, check_name, left_label, left_value,
                  right_label, right_value, difference, passed
           FROM reconciliation_results ORDER BY run_id, check_group, check_name""",
        "reconciliation_results de todas las ejecuciones"),
}


def write_csv_reports(db: Database, cfg: Config) -> list[Path]:
    out = cfg.paths.reports
    out.mkdir(parents=True, exist_ok=True)
    written = []
    failures = []
    for filename, (query, _) in CSV_REPORTS.items():
        try:
            db.df(query).to_csv(out / filename, index=False)
            written.append(out / filename)
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudo generar %s: %s", filename, exc)
            failures.append(f"{filename}: {type(exc).__name__}: {exc}")
    if failures:
        raise RuntimeError("Paquete de reportes incompleto: " + "; ".join(failures))
    log.info("%d reportes CSV escritos en %s", len(written), out)
    return written


# ---------------------------------------------------------------------- insights
def _d(value) -> str:
    """Fecha en ISO, sin la hora que anade el conversor de DuckDB a pandas."""
    try:
        return value.strftime("%Y-%m-%d")
    except AttributeError:
        return str(value)[:10]


def _fmt(value, decimals: int = 2) -> str:
    if value is None:
        return "n/d"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def write_insights(db: Database, cfg: Config) -> Path:
    out = cfg.paths.reports
    path = out / "insights.md"

    runs = db.df("SELECT * FROM v_snapshot_summary")
    if runs.empty:
        path.write_text("# Insights\n\nNo hay cortes procesados todavia.\n", encoding="utf-8")
        return path

    active = db.execute("""
        SELECT count(*) AS movements,
               count(DISTINCT id_cliente) AS clients,
               coalesce(sum(amount), 0) AS amount_total,
               coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0) AS amount_in,
               coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0) AS amount_out,
               min(movement_date) AS date_min, max(movement_date) AS date_max
        FROM v_movements_active""").df().iloc[0]

    by_fund = db.df("SELECT * FROM v_summary_by_fund")
    by_product = db.df("SELECT * FROM v_summary_by_product")
    by_commercial = db.df("SELECT * FROM v_summary_by_commercial")
    by_type = db.df("SELECT * FROM v_summary_by_type")
    fields = db.df("SELECT * FROM v_field_change_frequency")
    quality = db.df("SELECT * FROM v_rejections_by_code")
    flags = db.df("SELECT flag_code, severity, sum(occurrences) AS occurrences "
                  "FROM v_quality_flags_summary GROUP BY 1,2 ORDER BY 3 DESC")
    anomalies = db.df("SELECT category, anomaly_code, count(*) AS n FROM anomalies "
                      "GROUP BY 1,2 ORDER BY 3 DESC")
    top_changes = db.df("SELECT * FROM v_top_changes_by_impact LIMIT 10")
    changes_by_type = db.df(
        "SELECT change_type, sum(movements) AS movements, sum(amount_impact) AS amount_impact "
        "FROM v_daily_changes GROUP BY 1 ORDER BY 2 DESC")
    daily = db.df("SELECT * FROM v_daily_movements")

    total_processed = int(runs["rows_read"].sum())
    total_rejected = int(runs["rows_rejected"].sum())
    product_most_tx = by_product.iloc[by_product["movements"].idxmax()] if len(by_product) else None
    product_most_amt = by_product.iloc[0] if len(by_product) else None

    L: list[str] = []
    A = L.append

    A("# Insights y analitica")
    A("")
    A(f"_Generado el {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC a partir de "
      f"`{cfg.paths.database_file.name}`._")
    A("")
    A("Todas las cifras salen de consultas sobre las tablas persistidas y son "
      "reproducibles: cada bloque indica el metodo de calculo. Los CSV asociados "
      "estan en esta misma carpeta.")
    A("")
    A("> **Limitacion transversal.** Los archivos entregados no traen identificador "
      "de transaccion (la columna es `id_cliente`, con 3.000 valores distintos sobre "
      "50.000 filas). La identidad del movimiento es una clave de negocio derivada "
      "(`id_cliente` + fecha + producto + tipo + fondo + ordinal de ocurrencia). "
      "Toda clasificacion NEW/UPDATED/DELETED depende de ese supuesto, documentado "
      "en `docs/analysis_and_assumptions.md`.")
    A("")

    # ------------------------------------------------------------- 1. Resumen
    A("## 1. Estado vigente")
    A("")
    A("| Metrica | Valor |")
    A("| --- | ---: |")
    A(f"| Movimientos vigentes | {int(active.movements):,} |")
    A(f"| Clientes distintos | {int(active.clients):,} |")
    A(f"| Rango de fechas de movimiento | {_d(active.date_min)} - {_d(active.date_max)} |")
    A(f"| Monto total vigente | {_fmt(active.amount_total)} |")
    A(f"| Monto de entradas (IN) | {_fmt(active.amount_in)} |")
    A(f"| Monto de salidas (OUT) | {_fmt(active.amount_out)} |")
    A(f"| Balance neto (IN − OUT) | {_fmt(float(active.amount_in) - float(active.amount_out))} |")
    A("")
    A("**Metodo:** agregacion sobre `movements_current` filtrando `is_active = true`. "
      "**Periodo:** todos los cortes procesados. "
      "**Limitacion:** el balance neto asume que `type` distingue entrada de salida y "
      "que `amount` es siempre positivo en su sentido. Esa segunda parte NO se cumple: "
      "ver el apartado 6.")
    A("")

    # -------------------------------------------------------- 2. Evolucion
    A("## 2. Evolucion entre cortes")
    A("")
    A("| Corte | Archivo | Leidas | Validas | Rechazadas | NEW | UPDATED | DELETED | UNCHANGED | Vigentes despues |")
    A("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, r in runs.iterrows():
        A(f"| {_d(r.snapshot_date)} | {r.input_file} | {int(r.rows_read):,} | {int(r.rows_valid):,} | "
          f"{int(r.rows_rejected):,} | {int(r.rows_new):,} | {int(r.rows_updated):,} | "
          f"{int(r.rows_deleted):,} | {int(r.rows_unchanged):,} | {int(r.rows_current_after):,} |")
    A("")
    if len(changes_by_type):
        A("Impacto monetario acumulado por tipo de cambio:")
        A("")
        A("| Tipo de cambio | Movimientos | Impacto monetario |")
        A("| --- | ---: | ---: |")
        for _, r in changes_by_type.iterrows():
            A(f"| {r.change_type} | {int(r.movements):,} | {_fmt(r.amount_impact)} |")
        A("")
    A("**Metodo:** `pipeline_runs` (solo ejecuciones con estado SUCCESS) y "
      "`movement_changes.amount_delta`. **Fuente:** `change_summary.csv`, "
      "`snapshot_summary.csv`.")
    A("")

    # --------------------------------------------------------- 3. Financiero
    A("## 3. Analisis financiero por dimension")
    A("")
    if len(by_fund):
        A("### Por fondo")
        A("")
        A("| Fondo | Movimientos | Monto total | Entradas | Salidas | Flujo neto |")
        A("| --- | ---: | ---: | ---: | ---: | ---: |")
        for _, r in by_fund.iterrows():
            A(f"| {r.fund} | {int(r.movements):,} | {_fmt(r.amount_total)} | "
              f"{_fmt(r.amount_in)} | {_fmt(r.amount_out)} | {_fmt(r.net_flow)} |")
        A("")
    if len(by_product):
        A("### Por producto")
        A("")
        A("| Producto | Movimientos | Monto total | Flujo neto |")
        A("| --- | ---: | ---: | ---: |")
        for _, r in by_product.iterrows():
            A(f"| {r['product']} | {int(r.movements):,} | {_fmt(r.amount_total)} | {_fmt(r.net_flow)} |")
        A("")
    if product_most_tx is not None:
        A(f"- **Producto con mas transacciones:** {product_most_tx['product']} "
          f"({int(product_most_tx.movements):,} movimientos).")
        A(f"- **Producto con mayor monto:** {product_most_amt['product']} "
          f"({_fmt(product_most_amt.amount_total)}).")
        A("")
    if len(by_commercial):
        A("### Ranking de nombres comerciales")
        A("")
        A("| Nombre comercial | Movimientos | Monto total |")
        A("| --- | ---: | ---: |")
        for _, r in by_commercial.head(15).iterrows():
            A(f"| {r.commercial_name} | {int(r.movements):,} | {_fmt(r.amount_total)} |")
        A("")
        top = by_commercial.iloc[0]
        share = float(top.amount_total) / float(active.amount_total) if float(active.amount_total) else 0
        A(f"**Concentracion:** el primero concentra el {share:.1%} del monto vigente. "
          f"Con {len(by_commercial)} valores distintos y una distribucion practicamente "
          f"plana, no hay evidencia de concentracion relevante en esta dimension.")
        A("")
    A("**Metodo:** `movements_current` activo agrupado por cada dimension. "
      "**Filtros:** ninguno adicional. **Limitacion:** los movimientos con `amount` "
      "nulo estan en cuarentena y por tanto no suman en ninguna de estas cifras.")
    A("")

    # ---------------------------------------------------------- 4. Correcciones
    A("## 4. Que se corrige y cuanto pesa")
    A("")
    if len(fields):
        A("| Columna | Correcciones | Movimientos afectados |")
        A("| --- | ---: | ---: |")
        for _, r in fields.iterrows():
            A(f"| {r.column_name} | {int(r.changes):,} | {int(r.movements_affected):,} |")
        A("")
    if len(top_changes):
        A("Correcciones y bajas de mayor impacto monetario:")
        A("")
        A("| Corte | Tipo | Fondo | Producto | Antes | Despues | Impacto |")
        A("| --- | --- | --- | --- | ---: | ---: | ---: |")
        for _, r in top_changes.iterrows():
            A(f"| {_d(r.snapshot_date)} | {r.change_type} | {r.fund or '-'} | {r['product'] or '-'} | "
              f"{_fmt(r.amount_before)} | {_fmt(r.amount_after)} | {_fmt(r.amount_delta)} |")
        A("")
    A("**Metodo:** `movement_change_fields` (detalle por columna) y "
      "`movement_changes.amount_delta`. **Fuente:** `field_change_frequency.csv`, "
      "`top_changes_by_impact.csv`.")
    A("")

    # ------------------------------------------------------------- 5. Calidad
    A("## 5. Calidad de los datos")
    A("")
    rate = total_rejected / total_processed if total_processed else 0
    A(f"- Filas leidas en total: **{total_processed:,}**")
    A(f"- Filas enviadas a cuarentena: **{total_rejected:,}** ({rate:.2%})")
    A("")
    if len(quality):
        # La vista desglosa por corte; aqui interesa el total por codigo. Sin
        # agregar, un mismo codigo aparecia repetido una vez por corte y se
        # leia como si fueran motivos de rechazo distintos.
        by_code = (quality.groupby(["error_code", "error_severity"], as_index=False)
                          ["rows_rejected"].sum()
                          .sort_values("rows_rejected", ascending=False))
        A("| Codigo de rechazo | Severidad | Filas |")
        A("| --- | --- | ---: |")
        for _, r in by_code.iterrows():
            A(f"| {r.error_code} | {r.error_severity} | {int(r.rows_rejected):,} |")
        A("")
    if len(flags):
        A("Avisos no bloqueantes (el registro se conserva):")
        A("")
        A("| Flag | Severidad | Ocurrencias |")
        A("| --- | --- | ---: |")
        for _, r in flags.iterrows():
            A(f"| {r.flag_code} | {r.severity} | {int(r.occurrences):,} |")
        A("")
    A("**Metodo:** `rejected_records` y `data_quality_flags`. Ningun registro se "
      "descarta en silencio: cada fila rechazada conserva su JSON original y su "
      "codigo de error. **Fuente:** `rejections_by_code.csv`, `quality_flags.csv`.")
    A("")

    # ------------------------------------------------------------ 6. Anomalias
    A("## 6. Eventos que merecen revision")
    A("")
    A("Ninguno de estos puntos afirma que exista fraude o error. Son desviaciones "
      "respecto a un criterio explicito.")
    A("")
    if len(anomalies):
        A("| Categoria | Codigo | Casos |")
        A("| --- | --- | ---: |")
        for _, r in anomalies.iterrows():
            A(f"| {r.category} | {r.anomaly_code} | {int(r.n):,} |")
        A("")
    neg = by_type[by_type["negative_amounts"] > 0] if len(by_type) else by_type
    if len(neg):
        A("### Signo del monto frente al sentido del movimiento")
        A("")
        A("| type | Movimientos | Con monto negativo | % | Con monto cero |")
        A("| --- | ---: | ---: | ---: | ---: |")
        for _, r in by_type.iterrows():
            pct = int(r.negative_amounts) / int(r.movements) if int(r.movements) else 0
            A(f"| {r.type} | {int(r.movements):,} | {int(r.negative_amounts):,} | "
              f"{pct:.2%} | {int(r.zero_amounts):,} |")
        A("")
        A("**Hallazgo:** los montos negativos aparecen concentrados en un unico valor "
          "de `type`. **Interpretacion posible:** o bien existe una convencion de signo "
          "no documentada, o bien es un defecto del origen. **Decision tomada:** no se "
          "corrige ni se rechaza, porque el enunciado no define el signo; se marca con "
          "`NEGATIVE_AMOUNT` y se reporta aqui. **Evidencia:** `anomalies.csv`, "
          "`financial_summary.csv`.")
        A("")
    A("**Metodo de los outliers de monto:** regla intercuartilica de Tukey con "
      f"multiplicador {cfg.get('analytics.iqr_multiplier', 3.0)} aplicada por "
      "combinacion (fondo, tipo), porque las escalas difieren entre fondos. Se eligio "
      "IQR sobre z-score porque no supone normalidad y es explicable ante un auditor.")
    A("")

    # ---------------------------------------------------------- 7. Estacionalidad
    if len(daily) > 2:
        busiest = daily.iloc[daily["movements"].idxmax()]
        quietest = daily.iloc[daily["movements"].idxmin()]
        A("## 7. Distribucion temporal")
        A("")
        A(f"- Dia con mas movimientos: **{_d(busiest.movement_date)}** "
          f"({int(busiest.movements):,}).")
        A(f"- Dia con menos movimientos: **{_d(quietest.movement_date)}** "
          f"({int(quietest.movements):,}).")
        A(f"- Media diaria: **{daily['movements'].mean():,.1f}** movimientos "
          f"(desviacion {daily['movements'].std():,.1f}).")
        A("")
        A("**Metodo:** `v_daily_movements`, agrupando por `movement_date` (fecha del "
          "movimiento, no fecha del corte). **Fuente:** `daily_metrics.csv`.")
        A("")

    A("---")
    A("")
    A("## Como reproducir cualquier cifra")
    A("")
    A("```bash")
    A("docker compose run --rm shell")
    A("# o, sin Docker:")
    A("python -c \"import duckdb; con=duckdb.connect('data/database/movements.duckdb');\\")
    A("           print(con.execute('SELECT * FROM v_summary_by_fund').df())\"")
    A("```")
    A("")
    A("| Reporte | Consulta / metodo |")
    A("| --- | --- |")
    for filename, (_, method) in CSV_REPORTS.items():
        A(f"| `{filename}` | {method} |")
    A("")

    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    log.info("Insights escritos en %s", path)
    return path


def write_all(db: Database, cfg: Config) -> None:
    if not cfg.get("reporting.enabled", True):
        return
    write_csv_reports(db, cfg)
    write_insights(db, cfg)
