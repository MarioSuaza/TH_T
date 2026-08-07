"""Denominador de la metrica de bajas.

Regresion de un defecto real: `analytics.py` calculaba el porcentaje de bajas
sobre `rows_current_after + rows_deleted`. Ese denominador no es el vigente
previo -- `rows_current_after` ya incluye las altas del propio corte -- y
producia 18,3% donde el valor correcto es 22,0%.

Sobre los archivos entregados:
    10.658 / (47.560 + 10.658) = 18,31%   <- incorrecto
    10.658 / 48.457             = 22,00%   <- vigentes antes del corte

El pipeline reportaba 21,99% en el log y 18,31% en la anomalia: dos modulos
calculando la misma magnitud de forma distinta.
"""

from __future__ import annotations

from src.analytics import build_analytics
from tests.conftest import row


def test_rows_current_before_is_persisted(db, load, make_parquet):
    """Sin este dato, analytics no puede reconstruir el vigente previo."""
    load(make_parquet([row(id_cliente="CLI000001"),
                       row(id_cliente="CLI000002")]), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001")], name="t1.parquet"), "2024-09-16")

    runs = db.df("SELECT snapshot_date, rows_current_before, rows_current_after, "
                 "rows_deleted FROM pipeline_runs WHERE status = 'SUCCESS' "
                 "ORDER BY snapshot_date")

    assert runs.iloc[0]["rows_current_before"] == 0, "el primer corte parte de cero"
    assert runs.iloc[0]["rows_current_after"] == 2
    assert runs.iloc[1]["rows_current_before"] == 2, "vigentes ANTES de aplicar T+1"
    assert runs.iloc[1]["rows_deleted"] == 1


def test_deletion_share_uses_previous_active_as_denominator(db, load, make_parquet):
    """5 vigentes y 2 bajas dan 40%; la formula antigua daba 28,6%."""
    load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 6)]),
         "2024-09-15")
    # Llegan 3 de los 5 originales + 2 nuevos: 2 bajas sobre 5 vigentes previos.
    load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in (1, 2, 3)] +
                      [row(id_cliente="CLI000009"), row(id_cliente="CLI000010")],
                      name="t1.parquet"), "2024-09-16")

    run = db.df("SELECT rows_current_before, rows_deleted, rows_current_after "
                "FROM pipeline_runs WHERE snapshot_date = '2024-09-16'").iloc[0]

    assert run["rows_current_before"] == 5
    assert run["rows_deleted"] == 2
    assert run["rows_current_after"] == 5   # 3 supervivientes + 2 altas

    correct = run["rows_deleted"] / run["rows_current_before"]
    wrong = run["rows_deleted"] / (run["rows_current_after"] + run["rows_deleted"])

    assert correct == 0.40
    assert abs(wrong - 0.2857) < 0.001, "la formula antigua daba otro numero"


def test_anomaly_reports_the_corrected_share(db, cfg, load, make_parquet):
    """La anomalia publicada debe usar el vigente previo y declararlo."""
    load(make_parquet([row(id_cliente=f"CLI0000{i:02d}") for i in range(1, 11)]),
         "2024-09-15")
    load(make_parquet([row(id_cliente=f"CLI0000{i:02d}") for i in range(1, 8)],
                      name="t1.parquet"), "2024-09-16")

    # Las anomalias se calculan al cierre del pipeline, no por corte.
    build_analytics(db, cfg)

    anomaly = db.df("SELECT observed, description FROM anomalies "
                    "WHERE anomaly_code = 'HIGH_DELETION_SHARE'")

    assert len(anomaly) == 1, "3 bajas sobre 10 vigentes supera el umbral de 10%"
    assert abs(anomaly.iloc[0]["observed"] - 0.30) < 1e-9
    assert "10 vigentes antes del corte" in anomaly.iloc[0]["description"], \
        "la descripcion debe declarar el denominador usado"
