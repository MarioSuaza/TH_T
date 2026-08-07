"""Conexion a DuckDB, DDL idempotente y utilidades transaccionales.

Por que DuckDB (resumen; justificacion completa en docs/decisions.md ADR-001):
- Lee Parquet directamente con predicate y projection pushdown, sin cargar el
  archivo entero en memoria de Python.
- SQL analitico completo (FULL OUTER JOIN, window functions, QUALIFY) que es
  exactamente la primitiva que este problema necesita.
- DECIMAL exacto hasta 38 digitos: no hay riesgo de error de coma flotante en
  los montos persistidos.
- Transacciones ACID sobre un unico archivo, sin servicio adicional en el
  docker compose.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Iterator

import duckdb

from src.config import Config
from src.logging_config import get_logger

log = get_logger("database")

SCHEMA_SQL = "sql/schema.sql"


class Database:
    """Envoltorio delgado sobre duckdb.DuckDBPyConnection."""

    def __init__(self, cfg: Config, path: str | Path | None = None,
                 read_only: bool = False):
        self.cfg = cfg
        self.path = Path(path) if path else cfg.paths.database_file
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not read_only:
                self._clear_empty_files()
        self.con = duckdb.connect(str(self.path), read_only=read_only)
        self._tune()

    def _clear_empty_files(self) -> None:
        """Elimina un archivo de base o WAL de 0 bytes.

        Un proceso interrumpido (contenedor matado, disco lleno, Ctrl-C en el
        momento justo) puede dejar un archivo creado pero vacio. DuckDB lo
        rechaza con "not a valid DuckDB database file" y el pipeline quedaria
        bloqueado sin que el archivo contenga nada que perder.
        """
        for candidate in (self.path, self.path.with_suffix(self.path.suffix + ".wal")):
            try:
                if candidate.exists() and candidate.stat().st_size == 0:
                    candidate.unlink()
                    log.warning("Se elimino %s: estaba vacio, probablemente por una "
                                "ejecucion interrumpida.", candidate.name)
            except OSError as exc:
                log.error("No se pudo eliminar el archivo vacio %s: %s. "
                          "Borrelo manualmente antes de reintentar.", candidate, exc)

    # ------------------------------------------------------------------ setup
    def _tune(self) -> None:
        cfg = self.cfg
        self.con.execute("PRAGMA enable_progress_bar = false")

        # El pipeline no depende del orden de insercion en ningun punto:
        # desactivarlo reduce memoria y habilita mas paralelismo.
        if not cfg.get("database.preserve_insertion_order", False):
            self.con.execute("SET preserve_insertion_order = false")

        threads = cfg.get("database.threads")
        if threads:
            self.con.execute(f"SET threads = {int(threads)}")

        # DuckDB no lee los limites del cgroup: dentro de un contenedor con
        # memoria acotada hay que decirselo explicitamente o intentara usar el
        # 80% de la RAM del host.
        memory_limit = cfg.get("database.memory_limit")
        if memory_limit:
            self.con.execute(f"SET memory_limit = '{memory_limit}'")

        # Con temp_directory fijado, una agregacion o un join que no cabe en
        # memoria se resuelve en disco en lugar de fallar. Es lo que permite
        # procesar cortes mayores que la RAM disponible.
        temp_dir = cfg.get("database.temp_directory")
        if temp_dir and str(self.path) != ":memory:":
            tmp = cfg._path(temp_dir) if hasattr(cfg, "_path") else Path(temp_dir)
            tmp.mkdir(parents=True, exist_ok=True)
            self.con.execute(f"SET temp_directory = '{tmp}'")

    def init_schema(self) -> None:
        ddl_path = self.cfg.root / SCHEMA_SQL
        ddl = ddl_path.read_text(encoding="utf-8")
        ddl = ddl.replace("{amount_type}", self.cfg.amount_sql_type)
        self.con.execute(ddl)
        log.info("Esquema inicializado en %s", self.path)

    # ------------------------------------------------------------- ejecucion
    def execute(self, sql: str, params: list | tuple | None = None):
        return self.con.execute(sql, params) if params is not None else self.con.execute(sql)

    def sql_file(self, relative_path: str, **fmt) -> str:
        """Lee un .sql del repo y sustituye placeholders {nombre}."""
        text = (self.cfg.root / relative_path).read_text(encoding="utf-8")
        fmt.setdefault("amount_type", self.cfg.amount_sql_type)
        for key, value in fmt.items():
            text = text.replace("{" + key + "}", str(value))
        return text

    def scalar(self, sql: str, params: list | tuple | None = None):
        row = self.execute(sql, params).fetchone()
        return None if row is None else row[0]

    def df(self, sql: str, params: list | tuple | None = None):
        return self.execute(sql, params).df()

    def table_exists(self, name: str) -> bool:
        return bool(self.scalar(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [name]))

    # ---------------------------------------------------------- transacciones
    @contextlib.contextmanager
    def transaction(self) -> Iterator["Database"]:
        """Unidad atomica. Cualquier excepcion revierte TODO el bloque.

        DuckDB no soporta transacciones anidadas: este context manager es el
        unico punto donde se abre una.
        """
        self.con.execute("BEGIN TRANSACTION")
        try:
            yield self
        except Exception:
            self.con.execute("ROLLBACK")
            log.error("Transaccion revertida (ROLLBACK)")
            raise
        else:
            self.con.execute("COMMIT")

    # ------------------------------------------------------------------ misc
    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.con.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
