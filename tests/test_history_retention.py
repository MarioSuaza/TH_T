"""Retencion del historico.

`movements_history` crece de forma monotona: con dos cortes sinteticos de 10M y
9,8M filas, la base completa medida alcanza ~14,38 GB. La retencion permite
acotarlo sin perder el estado vigente.

La propiedad critica: purgar NUNCA debe borrar la version vigente de una clave.
Si lo hiciera, el estado actual dejaria de ser reconstruible desde el historico
y la invariante `history_covers_current` fallaria.
"""

from __future__ import annotations

import pytest

from src.persistence import check_invariants, prune_history
from tests.conftest import row


def test_retention_disabled_by_default(db, cfg, load, make_parquet):
    """Sin configurar, no se borra nada: el historico completo se conserva."""
    load(make_parquet([row(amount=100.00)]), "2024-09-15")
    load(make_parquet([row(amount=200.00)], name="t1.parquet"), "2024-09-16")

    antes = db.scalar("SELECT count(*) FROM movements_history")
    assert prune_history(db, cfg, snapshot_date=__import__("datetime").date(2030, 1, 1)) == 0
    assert db.scalar("SELECT count(*) FROM movements_history") == antes == 2


def test_retention_removes_only_closed_versions(db, cfg, load, make_parquet,
                                                monkeypatch):
    """Con retencion activa se borra la version cerrada, no la vigente."""
    from datetime import date

    load(make_parquet([row(amount=100.00)]), "2024-09-15")
    load(make_parquet([row(amount=200.00)], name="t1.parquet"), "2024-09-16")

    assert db.scalar("SELECT count(*) FROM movements_history") == 2
    assert db.scalar("SELECT count(*) FROM movements_history WHERE is_current") == 1

    monkeypatch.setitem(cfg._cfg["persistence"], "history_retention_days", 30)
    borradas = prune_history(db, cfg, snapshot_date=date(2024, 12, 31))

    assert borradas == 1, "la version cerrada en 2024-09-16 supera los 30 dias"
    assert db.scalar("SELECT count(*) FROM movements_history") == 1
    assert db.scalar("SELECT count(*) FROM movements_history WHERE is_current") == 1, \
        "la version vigente NUNCA se borra"


def test_retention_keeps_current_state_reconstructible(db, cfg, load,
                                                       make_parquet, monkeypatch):
    """Tras purgar, las invariantes estructurales siguen cumpliendose."""
    from datetime import date

    load(make_parquet([row(id_cliente="CLI000001", amount=100.00),
                       row(id_cliente="CLI000002", amount=300.00)]), "2024-09-15")
    load(make_parquet([row(id_cliente="CLI000001", amount=200.00),
                       row(id_cliente="CLI000002", amount=300.00)],
                      name="t1.parquet"), "2024-09-16")

    monkeypatch.setitem(cfg._cfg["persistence"], "history_retention_days", 30)
    prune_history(db, cfg, snapshot_date=date(2024, 12, 31))

    violaciones = check_invariants(db)
    assert violaciones == [], f"la purga rompio invariantes: {violaciones}"

    # El estado vigente sigue completo y correcto.
    assert db.scalar("SELECT count(*) FROM movements_current WHERE is_active") == 2
    assert float(db.scalar(
        "SELECT amount FROM movements_current WHERE id_cliente = 'CLI000001'")) == 200.00


def test_staging_is_emptied_after_a_successful_load(db, load, make_parquet):
    """Silver es transitoria: no debe quedar residente tras el corte.

    Se vaciaba al INICIO de cada ejecucion, lo que dejaba el ultimo corte en
    disco indefinidamente (~33% del espacio de la base).
    """
    load(make_parquet([row(id_cliente=f"CLI00000{i}") for i in range(1, 6)]),
         "2024-09-15")

    assert db.scalar("SELECT count(*) FROM stg_movements") == 0, \
        "stg_movements debe quedar vacia cuando el corte termina bien"

    # El estado vigente si persiste: no se borro nada que hiciera falta.
    assert db.scalar("SELECT count(*) FROM movements_current") == 5


def test_retention_respects_recent_versions(db, cfg, load, make_parquet,
                                            monkeypatch):
    """Una version cerrada dentro de la ventana de retencion no se toca."""
    from datetime import date

    load(make_parquet([row(amount=100.00)]), "2024-09-15")
    load(make_parquet([row(amount=200.00)], name="t1.parquet"), "2024-09-16")

    monkeypatch.setitem(cfg._cfg["persistence"], "history_retention_days", 365)
    # Solo 10 dias despues del cierre: dentro de la ventana.
    assert prune_history(db, cfg, snapshot_date=date(2024, 9, 26)) == 0
    assert db.scalar("SELECT count(*) FROM movements_history") == 2


def test_reconciliation_survives_purge_that_shifts_min_version(
        db, cfg, load, make_parquet, monkeypatch):
    """La reconciliacion no debe fallar cuando la retencion ya purgo versiones
    antiguas y la secuencia conservada empieza en una version > 1.

    Reproduce el caso reportado: varios cortes sobre la misma clave con
    retencion de 30 dias activa desde el primer corte. Para cuando se aplica
    el ultimo corte, la version 1 ya fue purgada (quedo cerrada y fuera de la
    ventana de retencion tras un corte posterior lo bastante alejado en el
    tiempo) y solo quedan versiones intermedias en movements_history.
    `no_version_gaps` en persistence.py tolera esto (max - min + 1 = count),
    pero reconciliation.sql exigia max(version) = count(*), que deja de
    cumplirse en cuanto min(version) > 1, y el corte fallaba con
    RECONCILIATION_FAILED aunque no hay ninguna inconsistencia real.
    """
    monkeypatch.setitem(cfg._cfg["persistence"], "history_retention_days", 30)

    # Corte 1: version 1. Se cierra (valid_to = 2024-02-15) al llegar el
    # corte 2, pero en ese momento todavia esta dentro de la ventana de
    # retencion medida contra el propio corte 2, asi que no se purga aun.
    load(make_parquet([row(amount=100.00)]), "2024-01-01")
    load(make_parquet([row(amount=200.00)], name="t1.parquet"), "2024-02-15")
    assert db.scalar("SELECT count(*) FROM movements_history") == 2

    # Corte 3, mas de 30 dias despues del cierre de la version 1: prune_history
    # la purga dentro de la misma transaccion que reconcilia. La reconciliacion
    # debe tolerar que la secuencia de versiones ya no empiece en 1.
    result = load(make_parquet([row(amount=300.00)], name="t2.parquet"),
                  "2024-03-25")
    assert result.status == "SUCCESS"
    assert db.scalar("SELECT count(*) FROM movements_history") == 2, \
        "la version 1 debio purgarse: ya esta cerrada y fuera de ventana"
    assert db.scalar("SELECT min(version) FROM movements_history") == 2

    # Corte 4: sigue funcionando con normalidad tras la purga.
    result = load(make_parquet([row(amount=400.00)], name="t3.parquet"),
                  "2024-03-26")
    assert result.status == "SUCCESS"
