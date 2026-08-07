"""Logging estructurado con run_id en cada linea.

No se usa `print` en el pipeline: todo pasa por logging, con nivel, timestamp,
run_id y modulo de origen, de forma que un fallo sea localizable.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from pathlib import Path

_run_id: ContextVar[str] = ContextVar("run_id", default="-")


def set_run_id(run_id: str) -> None:
    _run_id.set(run_id)


def get_run_id() -> str:
    return _run_id.get()


class _RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "run_id": getattr(record, "run_id", "-"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key in ("event", "metric", "value", "error_code"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)


_TEXT_FMT = "%(asctime)s | %(levelname)-8s | run=%(run_id)s | %(name)-22s | %(message)s"

_configured = False


def setup_logging(level: str = "INFO", fmt: str = "text",
                  logfile: str | Path | None = None) -> None:
    global _configured
    root = logging.getLogger()
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = _JsonFormatter() if fmt == "json" else logging.Formatter(
        _TEXT_FMT, datefmt="%Y-%m-%dT%H:%M:%S")
    flt = _RunIdFilter()

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    stream.addFilter(flt)
    root.addHandler(stream)

    if logfile:
        path = Path(logfile)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(formatter)
        fh.addFilter(flt)
        root.addHandler(fh)

    # DuckDB y librerias de terceros: solo avisos hacia arriba.
    logging.getLogger("duckdb").setLevel(logging.WARNING)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    if not _configured:
        setup_logging()
    return logging.getLogger(name)
