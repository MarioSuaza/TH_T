"""Publicacion de reportes y propagacion de fallos."""

from __future__ import annotations

import pytest

from src import reporting


def test_un_csv_fallido_hace_visible_el_paquete_incompleto(cfg, monkeypatch):
    class Frame:
        def __init__(self, fail: bool):
            self.fail = fail

        def to_csv(self, path, index=False):
            if self.fail:
                raise OSError("sin espacio")
            path.write_text("ok\n", encoding="utf-8")

    class FakeDatabase:
        def df(self, query):
            return Frame(fail=query == "bad")

    monkeypatch.setattr(reporting, "CSV_REPORTS", {
        "ok.csv": ("ok", "control"),
        "bad.csv": ("bad", "control"),
    })

    with pytest.raises(RuntimeError, match="Paquete de reportes incompleto.*bad.csv"):
        reporting.write_csv_reports(FakeDatabase(), cfg)
