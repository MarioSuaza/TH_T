"""Tablero de inspeccion de resultados (Streamlit).

Cada grafico responde a una pregunta concreta, escrita encima de el. No hay
visualizaciones decorativas.

La base se abre en modo SOLO LECTURA: el tablero no puede alterar el resultado
del pipeline.

    streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config  # noqa: E402
from src.presentation import en_miles_de_millones, por_fecha  # noqa: E402

st.set_page_config(page_title="Movimientos financieros - Tyba", layout="wide",
                   initial_sidebar_state="expanded")

CFG = load_config()
DB_PATH = CFG.paths.database_file


# --------------------------------------------------------------------- datos
@st.cache_resource
def _connection():
    """Conexion en solo lectura, o None si la base no esta utilizable.

    Se distinguen tres situaciones porque la accion del usuario es distinta en
    cada una, y ninguna debe salir como un traceback:
      - la base no existe            -> hay que ejecutar el pipeline
      - existe pero esta vacia       -> ejecucion interrumpida; el pipeline la
                                        limpia solo al arrancar
      - existe pero no es una base   -> archivo corrupto o ajeno
    El tablero NUNCA borra el archivo: es un consumidor de solo lectura.
    """
    import duckdb

    if not DB_PATH.exists():
        return None
    if DB_PATH.stat().st_size == 0:
        return "EMPTY"
    try:
        return duckdb.connect(str(DB_PATH), read_only=True)
    except duckdb.IOException:
        return "INVALID"
    except Exception:  # noqa: BLE001
        return "INVALID"


@st.cache_data(ttl=60)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = _connection()
    if con is None or isinstance(con, str):
        return pd.DataFrame()
    try:
        return con.execute(sql, list(params)).df()
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Consulta no disponible: {exc}")
        return pd.DataFrame()


def money(value) -> str:
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "n/d"


# --------------------------------------------------------------------- guardas
_conn = _connection()

if _conn is None:
    st.error(
        f"No existe la base de datos en `{DB_PATH.name}`.\n\n"
        "Ejecuta primero el pipeline:\n\n"
        "```bash\ndocker compose up --build\n```")
    st.stop()

if _conn == "EMPTY":
    st.error(
        f"El archivo `{DB_PATH.name}` existe pero esta vacio (0 bytes). "
        "Suele ser el rastro de una ejecucion interrumpida.\n\n"
        "El pipeline lo elimina solo al arrancar, asi que basta con volver a "
        "ejecutarlo:\n\n"
        "```bash\ndocker compose up --build\n```\n\n"
        "Si prefieres limpiarlo a mano:\n\n"
        "```bash\nrm -f data/database/movements.duckdb*\n```")
    st.stop()

if _conn == "INVALID":
    st.error(
        f"El archivo `{DB_PATH.name}` existe pero no es una base DuckDB valida.\n\n"
        "Puede estar corrupto o proceder de otra version. Elimina la base y "
        "vuelve a generarla:\n\n"
        "```bash\nrm -f data/database/movements.duckdb*\ndocker compose up --build\n```\n\n"
        "Los archivos de `data/raw/` no se tocan: el estado se reconstruye "
        "entero a partir de ellos.")
    st.stop()

runs = q("SELECT * FROM v_snapshot_summary")
if runs.empty:
    st.warning("Todavia no hay ningun corte procesado con exito.")
    st.stop()


# ------------------------------------------------------------------ cabecera
st.title("Movimientos financieros")
st.caption(f"Fuente: `{DB_PATH.name}` · solo lectura · "
           f"{len(runs)} corte(s) procesado(s)")

last_run = q("SELECT run_id, status, snapshot_date, input_file, finished_at, "
             "duration_seconds FROM pipeline_runs ORDER BY started_at DESC LIMIT 1")

tabs = st.tabs(["Resumen ejecutivo", "Evolucion temporal", "Analisis financiero",
                "Calidad de datos", "Explorador de cambios", "Eventos a revisar"])


# ============================================================ 1. RESUMEN
with tabs[0]:
    total = q("""
        SELECT count(*) AS movements,
               count(DISTINCT id_cliente) AS clients,
               coalesce(sum(amount), 0) AS amount_total,
               coalesce(sum(amount) FILTER (WHERE type = 'IN'), 0) AS amount_in,
               coalesce(sum(amount) FILTER (WHERE type = 'OUT'), 0) AS amount_out
        FROM v_movements_active""").iloc[0]
    acc = runs[["rows_new", "rows_updated", "rows_deleted", "rows_rejected"]].sum()

    c = st.columns(4)
    c[0].metric("Movimientos vigentes", f"{int(total.movements):,}")
    c[1].metric("Clientes distintos", f"{int(total.clients):,}")
    c[2].metric("Monto vigente", money(total.amount_total))
    c[3].metric("Balance neto (IN − OUT)",
                money(float(total.amount_in) - float(total.amount_out)))

    c = st.columns(4)
    c[0].metric("Altas acumuladas", f"{int(acc.rows_new):,}")
    c[1].metric("Correcciones acumuladas", f"{int(acc.rows_updated):,}")
    c[2].metric("Bajas acumuladas", f"{int(acc.rows_deleted):,}")
    c[3].metric("En cuarentena", f"{int(acc.rows_rejected):,}")

    c = st.columns(3)
    c[0].metric("Entradas (IN)", money(total.amount_in))
    c[1].metric("Salidas (OUT)", money(total.amount_out))
    if len(last_run):
        r = last_run.iloc[0]
        c[2].metric("Ultima ejecucion", str(r.status),
                    help=f"{r.run_id} · corte {str(r.snapshot_date)[:10]} · "
                         f"{r.duration_seconds:.2f}s")

    st.divider()
    st.subheader("¿Que aporto cada corte?")
    st.dataframe(
        runs[["snapshot_date", "input_file", "rows_read", "rows_valid", "rows_rejected",
              "rows_new", "rows_updated", "rows_deleted", "rows_unchanged",
              "rows_current_after"]],
        use_container_width=True, hide_index=True)

    alerts = q("SELECT run_id, alert_code, severity, message FROM run_alerts "
               "ORDER BY raised_at DESC")
    if len(alerts):
        st.subheader("Alertas emitidas")
        st.dataframe(alerts, use_container_width=True, hide_index=True)

    checks = q("SELECT run_id, check_group, check_name, left_value, right_value, "
               "difference, passed FROM reconciliation_results ORDER BY run_id, check_group")
    if len(checks):
        failed = int((~checks["passed"]).sum())
        st.subheader("Reconciliacion")
        if failed:
            st.error(f"{failed} control(es) no cuadran.")
        else:
            st.success(f"Los {len(checks)} controles de reconciliacion cuadran.")
        with st.expander("Ver el detalle"):
            st.dataframe(checks, use_container_width=True, hide_index=True)


# =================================================== 2. EVOLUCION TEMPORAL
with tabs[1]:
    daily = q("SELECT * FROM v_daily_movements")

    if len(daily):
        # El ultimo dia suele estar en curso cuando se toma el corte: su volumen
        # es una fraccion del de un dia cerrado. Se avisa una vez, arriba, en
        # lugar de repetir la advertencia en cada grafico.
        parciales = daily[daily["is_partial"]] if "is_partial" in daily else daily.iloc[0:0]
        if len(parciales):
            fechas = ", ".join(pd.to_datetime(parciales["movement_date"])
                               .dt.strftime("%Y-%m-%d"))
            st.info(
                f"**{fechas}** aparece con un volumen muy inferior al resto "
                f"({int(parciales['movements'].iloc[0]):,} movimientos frente a una "
                f"mediana de {int(daily['movements'].median()):,}). Es el ultimo dia "
                "observado y estaba en curso cuando se tomo el corte, no una caida "
                "del negocio: los movimientos de ese dia siguen llegando. Las series "
                "de abajo lo incluyen; tenlo en cuenta al leer la ultima barra.")

        d = por_fecha(daily, "movement_date")

        st.subheader("¿Cuantos movimientos hay cada dia, por sentido?")
        st.bar_chart(d[["movements_in", "movements_out"]], height=280)

        st.subheader("¿Cuanto dinero mueve cada dia, por sentido?")
        st.caption("En miles de millones. Entradas y salidas van muy parejas: la "
                   "diferencia entre ambas es el flujo neto, que se ve mejor en el "
                   "grafico siguiente.")
        st.bar_chart(en_miles_de_millones(d[["amount_in", "amount_out"]]), height=280)

        # net_flow tiene su propio panel y no comparte eje con amount_in/out: la
        # diferencia (~2 mil millones) es un 10% de las series que la originan, asi
        # que dibujarlas juntas la aplana hasta hacerla ilegible.
        st.subheader("¿El saldo del dia fue positivo o negativo?")
        st.caption("Entradas menos salidas, en miles de millones. Es la diferencia "
                   "entre las dos barras de arriba, en su propia escala.")
        st.bar_chart(en_miles_de_millones(d[["net_flow"]]), height=260)

    st.divider()
    st.subheader("¿Que cambios trajo cada corte?")
    changes = q("SELECT * FROM v_daily_changes")
    if len(changes):
        pivot = changes.pivot_table(index="snapshot_date", columns="change_type",
                                    values="movements", aggfunc="sum").fillna(0)
        pivot.index = pd.to_datetime(pivot.index).strftime("%Y-%m-%d")
        st.bar_chart(pivot, height=280)

        st.subheader("¿Cuanto dinero movio cada tipo de cambio?")
        impact = changes.pivot_table(index="snapshot_date", columns="change_type",
                                     values="amount_impact", aggfunc="sum").fillna(0)
        impact.index = pd.to_datetime(impact.index).strftime("%Y-%m-%d")
        # round() en lugar de Styler.format: el Styler exige jinja2, que no es
        # dependencia directa del proyecto.
        st.dataframe(impact.round(2).reset_index().rename(
            columns={"index": "snapshot_date"}),
            use_container_width=True, hide_index=True)

    st.subheader("¿Que proporcion del corte fueron correcciones?")
    if len(runs):
        rate = por_fecha(runs, "snapshot_date")[["updated_rate", "rejection_rate"]]
        st.bar_chart(rate, height=240)


# ==================================================== 3. ANALISIS FINANCIERO
with tabs[2]:
    dim = st.radio("Dimension", ["fund", "product", "commercial_name", "type"],
                   horizontal=True,
                   format_func={"fund": "Fondo", "product": "Producto",
                                "commercial_name": "Nombre comercial",
                                "type": "Tipo"}.get)
    view = {"fund": "v_summary_by_fund", "product": "v_summary_by_product",
            "commercial_name": "v_summary_by_commercial", "type": "v_summary_by_type"}[dim]
    df = q(f"SELECT * FROM {view}")

    if len(df):
        label = df.columns[0]
        st.subheader(f"¿Donde se concentra el monto por {label}?")
        st.bar_chart(df.set_index(label)[["amount_total"]], height=280)

        if "net_flow" in df.columns:
            st.subheader(f"¿Que {label} capta y cual pierde dinero neto?")
            st.bar_chart(df.set_index(label)[["net_flow"]], height=260)

        st.subheader("Detalle")
        st.dataframe(df, use_container_width=True, hide_index=True)

        total = float(df["amount_total"].sum())
        top = df.iloc[0]
        st.caption(f"El primero (`{top[label]}`) concentra el "
                   f"{float(top.amount_total) / total:.1%} del monto vigente.")

    st.divider()
    st.subheader("¿Como se distribuyen los importes?")
    dist = q("""
        SELECT CASE
                 WHEN amount < 0 THEN '1. negativo'
                 WHEN amount = 0 THEN '2. cero'
                 WHEN amount < 1000000 THEN '3. < 1M'
                 WHEN amount < 10000000 THEN '4. 1M - 10M'
                 WHEN amount < 25000000 THEN '5. 10M - 25M'
                 ELSE '6. >= 25M'
               END AS tramo,
               count(*) AS movements,
               coalesce(sum(amount), 0) AS amount_total
        FROM v_movements_active GROUP BY 1 ORDER BY 1""")
    if len(dist):
        st.bar_chart(dist.set_index("tramo")[["movements"]], height=240)
        st.dataframe(dist, use_container_width=True, hide_index=True)

    st.subheader("Movimientos vigentes de mayor importe")
    st.dataframe(q("SELECT * FROM v_top_movements LIMIT 50"),
                 use_container_width=True, hide_index=True)


# ======================================================== 4. CALIDAD DE DATOS
with tabs[3]:
    quality = q("SELECT * FROM v_quality_by_run")
    if len(quality):
        c = st.columns(3)
        c[0].metric("Filas leidas", f"{int(quality['rows_read'].sum()):,}")
        c[1].metric("Filas validas", f"{int(quality['rows_valid'].sum()):,}")
        c[2].metric("En cuarentena", f"{int(quality['rows_rejected'].sum()):,}",
                    delta=f"{quality['rejection_rate'].mean():.2%} medio",
                    delta_color="inverse")

        st.subheader("¿Que porcentaje de cada corte fue valido?")
        st.bar_chart(por_fecha(quality, "snapshot_date")[["valid_rate", "rejection_rate"]],
                     height=240)

    st.subheader("¿Por que se rechazaron registros?")
    rej = q("SELECT error_code, error_severity, sum(rows_rejected) AS rows "
            "FROM v_rejections_by_code GROUP BY 1, 2 ORDER BY 3 DESC")
    if len(rej):
        st.bar_chart(rej.set_index("error_code")[["rows"]], height=240)
        st.dataframe(rej, use_container_width=True, hide_index=True)
    else:
        st.success("Ningun registro fue rechazado.")

    st.subheader("Avisos no bloqueantes (el registro se conserva)")
    flags = q("SELECT flag_code, severity, column_name, sum(occurrences) AS occurrences "
              "FROM v_quality_flags_summary GROUP BY 1, 2, 3 ORDER BY 4 DESC")
    st.dataframe(flags, use_container_width=True, hide_index=True)

    st.subheader("Registros en cuarentena (muestra)")
    st.dataframe(q("SELECT snapshot_date, source_file, id_cliente, error_code, "
                   "error_description, raw_record FROM rejected_records LIMIT 200"),
                 use_container_width=True, hide_index=True)

    st.subheader("Trazabilidad de archivos")
    st.dataframe(q("SELECT source_file, snapshot_date, row_count, status, run_id, "
                   "substr(source_file_hash, 1, 16) AS hash_corto, size_bytes "
                   "FROM file_registry ORDER BY snapshot_date"),
                 use_container_width=True, hide_index=True)


# ==================================================== 5. EXPLORADOR DE CAMBIOS
with tabs[4]:
    st.subheader("Explorar los cambios detectados")

    opts = lambda sql: [r[0] for r in q(sql).itertuples(index=False)]  # noqa: E731

    f = st.columns(4)
    snaps = f[0].multiselect("Corte", opts(
        "SELECT DISTINCT CAST(snapshot_date AS VARCHAR) FROM movement_changes ORDER BY 1"))
    types = f[1].multiselect("Tipo de cambio", opts(
        "SELECT DISTINCT change_type FROM movement_changes ORDER BY 1"))
    funds = f[2].multiselect("Fondo", opts(
        "SELECT DISTINCT fund FROM movements_current ORDER BY 1"))
    products = f[3].multiselect("Producto", opts(
        "SELECT DISTINCT product FROM movements_current ORDER BY 1"))

    f2 = st.columns(4)
    movtypes = f2[0].multiselect("Sentido", ["IN", "OUT"])
    commercials = f2[1].multiselect("Nombre comercial", opts(
        "SELECT DISTINCT coalesce(commercial_name, '(sin nombre)') "
        "FROM movements_current ORDER BY 1"))
    dates = f2[2].multiselect("Fecha del movimiento", opts(
        "SELECT DISTINCT CAST(movement_date AS VARCHAR) FROM movements_current ORDER BY 1"))
    limit = f2[3].number_input("Filas a mostrar", 10, 5000, 300, step=10)

    bounds = q("SELECT coalesce(min(amount), 0) AS lo, coalesce(max(amount), 0) AS hi "
               "FROM movements_current")
    lo, hi = float(bounds.iloc[0].lo), float(bounds.iloc[0].hi)
    amount_range = st.slider("Rango de monto vigente", lo, hi, (lo, hi))

    where, params = ["1 = 1"], []
    if snaps:
        where.append(f"CAST(snapshot_date AS VARCHAR) IN ({','.join('?' * len(snaps))})")
        params += snaps
    if types:
        where.append(f"change_type IN ({','.join('?' * len(types))})")
        params += types
    if funds:
        where.append(f"fund IN ({','.join('?' * len(funds))})")
        params += funds
    if products:
        where.append(f"product IN ({','.join('?' * len(products))})")
        params += products
    if movtypes:
        where.append(f"type IN ({','.join('?' * len(movtypes))})")
        params += movtypes
    if commercials:
        where.append(f"coalesce(commercial_name, '(sin nombre)') "
                     f"IN ({','.join('?' * len(commercials))})")
        params += commercials
    if dates:
        where.append(f"CAST(movement_date AS VARCHAR) IN ({','.join('?' * len(dates))})")
        params += dates
    where.append("(amount_after IS NULL OR amount_after BETWEEN ? AND ?)")
    params += [amount_range[0], amount_range[1]]

    clause = " AND ".join(where)
    st.caption(f"{int(q(f'SELECT count(*) AS n FROM v_change_detail WHERE {clause}', tuple(params)).iloc[0].n):,} "
               f"cambio(s) coinciden con el filtro.")

    detail = q(f"""SELECT change_id, snapshot_date, change_type, id_cliente, movement_date,
                          product, fund, type, commercial_name, from_version, to_version,
                          changed_columns, amount_before, amount_after, amount_delta,
                          is_active, source_file, movement_key
                   FROM v_change_detail WHERE {clause}
                   ORDER BY abs(coalesce(amount_delta, 0)) DESC LIMIT {int(limit)}""",
               tuple(params))
    st.dataframe(detail, use_container_width=True, hide_index=True)

    st.subheader("Valores anteriores y nuevos, columna a columna")
    if len(detail):
        ids = detail["change_id"].tolist()
        fields = q(f"""SELECT f.change_id, f.movement_key, f.column_name,
                              f.old_value, f.new_value, mc.snapshot_date
                       FROM movement_change_fields f
                       JOIN movement_changes mc ON mc.change_id = f.change_id
                       WHERE f.change_id IN ({','.join('?' * len(ids))})
                       ORDER BY f.change_id""", tuple(ids))
        st.dataframe(fields, use_container_width=True, hide_index=True)

    st.subheader("¿Que columnas se corrigen mas a menudo?")
    freq = q("SELECT * FROM v_field_change_frequency")
    if len(freq):
        st.bar_chart(freq.set_index("column_name")[["changes"]], height=220)


# ==================================================== 6. EVENTOS A REVISAR
with tabs[5]:
    st.subheader("Eventos que merecen revision")
    st.caption("Ninguno de estos puntos afirma que exista fraude o error: son "
               "desviaciones respecto a un criterio explicito y configurable.")

    anomalies = q("SELECT category, anomaly_code, severity, entity_type, entity_id, "
                  "metric_name, observed, expected, threshold, description "
                  "FROM anomalies ORDER BY category, abs(coalesce(observed, 0)) DESC")
    if anomalies.empty:
        st.success("No se detectaron eventos que requieran revision.")
    else:
        summary = anomalies.groupby(["category", "anomaly_code"]).size() \
                           .reset_index(name="casos")
        st.dataframe(summary, use_container_width=True, hide_index=True)

        cats = st.multiselect("Categoria", sorted(anomalies["category"].unique()))
        view = anomalies[anomalies["category"].isin(cats)] if cats else anomalies
        st.dataframe(view.head(500), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Relacion observada entre el signo del monto y el sentido")
    by_type = q("SELECT * FROM v_summary_by_type")
    if len(by_type):
        st.dataframe(by_type, use_container_width=True, hide_index=True)
        st.caption("El enunciado no define el signo de `amount`. La solucion no "
                   "corrige ni rechaza los negativos: los marca y los reporta.")

    st.subheader("Correcciones y bajas de mayor impacto monetario")
    st.dataframe(q("SELECT * FROM v_top_changes_by_impact LIMIT 50"),
                 use_container_width=True, hide_index=True)
