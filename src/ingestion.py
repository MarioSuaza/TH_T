"""Ingesta: descubrimiento de archivos, hash, metadatos y validaciones de archivo/esquema.

La zona raw es INMUTABLE: este modulo solo lee. Nada se mueve, renombra ni
reescribe en data/raw.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq

from src.config import Config
from src.logging_config import get_logger

log = get_logger("ingestion")

READ_CHUNK = 1024 * 1024


class CriticalIngestionError(Exception):
    """Error critico de archivo o esquema: el pipeline debe detenerse."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class FileMetadata:
    path: Path
    name: str
    size_bytes: int
    mtime: datetime
    sha256: str
    row_count: int
    column_names: list[str]
    column_types: dict[str, str]
    schema_fingerprint: str
    snapshot_date: date | None = None
    snapshot_date_source: str = "unresolved"
    order_hint: int = 0
    issues: list[str] = field(default_factory=list)

    def as_registry_row(self, status: str, run_id: str | None = None) -> tuple:
        return (self.sha256, self.name, str(self.path), self.size_bytes, self.mtime,
                self.snapshot_date, self.row_count, self.column_names,
                self.schema_fingerprint, status, run_id)


# ---------------------------------------------------------------------- hash
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(READ_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------------------------------------------ metadata
def read_metadata(path: Path, cfg: Config) -> FileMetadata:
    """Lee metadatos del parquet SIN materializar los datos.

    pyarrow expone footer/schema sin leer los row groups: el coste es O(1)
    respecto al numero de filas.
    """
    if not path.exists():
        raise CriticalIngestionError("FILE_NOT_FOUND", f"No existe el archivo {path}")
    if not path.is_file():
        raise CriticalIngestionError("FILE_UNREADABLE", f"{path} no es un archivo")

    stat = path.stat()
    if stat.st_size == 0:
        raise CriticalIngestionError("FILE_UNREADABLE", f"{path} tiene 0 bytes")

    try:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        row_count = pf.metadata.num_rows
    except Exception as exc:  # noqa: BLE001 - se reclasifica como error critico
        raise CriticalIngestionError(
            "FILE_UNREADABLE", f"No se puede leer {path.name} como parquet: {exc}") from exc

    column_names = list(schema.names)
    column_types = {n: str(schema.field(n).type) for n in column_names}
    fingerprint = hashlib.sha256(
        json.dumps(sorted(column_types.items()), ensure_ascii=False).encode()
    ).hexdigest()[:16]

    if row_count == 0:
        raise CriticalIngestionError("FILE_EMPTY", f"{path.name} no contiene filas")

    return FileMetadata(
        path=path,
        name=path.name,
        size_bytes=stat.st_size,
        mtime=datetime.fromtimestamp(stat.st_mtime),
        sha256=sha256_file(path),
        row_count=row_count,
        column_names=column_names,
        column_types=column_types,
        schema_fingerprint=fingerprint,
    )


# ------------------------------------------------------------ validar esquema
def validate_schema(meta: FileMetadata, cfg: Config) -> None:
    """Validaciones CRITICAS de esquema. Una columna obligatoria ausente detiene todo."""
    required = set(cfg.required_columns)
    present = set(meta.column_names)

    missing = sorted(required - present)
    if missing:
        raise CriticalIngestionError(
            "MISSING_REQUIRED_COLUMN",
            f"{meta.name}: faltan columnas obligatorias {missing}. "
            f"Columnas recibidas: {sorted(present)}")

    extra = sorted(present - set(cfg.column_names))
    if extra:
        # Columnas adicionales NO son criticas: se ignoran y se registran.
        meta.issues.append(f"EXTRA_COLUMNS:{','.join(extra)}")
        log.warning("%s: columnas adicionales ignoradas: %s", meta.name, extra)

    # El orden de columnas es irrelevante (se accede por nombre), pero se avisa
    # si cambia respecto al contrato, porque suele indicar un cambio de origen.
    if [c for c in meta.column_names if c in required] != [c for c in cfg.column_names if c in required]:
        meta.issues.append("COLUMN_ORDER_DIFFERS")
        log.info("%s: el orden de columnas difiere del contrato (no bloqueante)", meta.name)

    # Compatibilidad de tipos fisicos: se comprueba la familia, no el tipo exacto.
    for col in cfg.columns:
        name = col["name"]
        if name not in meta.column_types:
            continue
        actual = meta.column_types[name]
        expected = col["physical_type"].upper()
        if not _type_compatible(expected, actual):
            raise CriticalIngestionError(
                "SCHEMA_MISMATCH",
                f"{meta.name}: columna {name} llega como {actual}, "
                f"incompatible con {expected} declarado en el contrato")


_NUMERIC = ("double", "float", "decimal", "int", "halffloat")
_STRINGY = ("string", "large_string", "utf8", "binary")


def _type_compatible(expected: str, actual: str) -> bool:
    a = actual.lower()
    # Una columna cuyos valores son todos nulos llega tipada como `null` en
    # parquet. Es un caso legitimo (un corte sin ninguna descripcion, por
    # ejemplo) y compatible con cualquier tipo destino.
    if a in ("null", "na"):
        return True
    if expected == "VARCHAR":
        # Un numerico tambien es aceptable para una columna textual: se castea.
        return any(t in a for t in _STRINGY) or any(t in a for t in _NUMERIC) or "date" in a or "timestamp" in a
    if expected == "DOUBLE":
        return any(t in a for t in _NUMERIC) or any(t in a for t in _STRINGY)
    return True


# ------------------------------------------------------- fecha del snapshot
def resolve_snapshot_date(meta: FileMetadata, cfg: Config,
                          cli_date: date | None = None) -> FileMetadata:
    """Determina la fecha del corte segun el orden de prioridad configurado.

    No se depende SOLO del nombre del archivo: si existe una fuente mas
    confiable (CLI o el propio contenido), se usa esa.
    """
    order = cfg.get("ingestion.snapshot_date_resolution", ["cli", "filename_iso"])
    declared = cfg.get("ingestion.declared_sequence", {}) or {}

    for source in order:
        if source == "cli" and cli_date:
            meta.snapshot_date, meta.snapshot_date_source = cli_date, "cli"
            return meta

        if source == "filename_iso":
            m = re.search(cfg.get("ingestion.filename_date_regex", r"(\d{4}-\d{2}-\d{2})"), meta.name)
            if m:
                try:
                    meta.snapshot_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                    meta.snapshot_date_source = "filename_iso"
                    return meta
                except ValueError:
                    pass

        if source == "declared_sequence" and meta.name in declared:
            entry = declared[meta.name]
            meta.snapshot_date = _as_date(entry["snapshot_date"])
            meta.order_hint = int(entry.get("order", 0))
            meta.snapshot_date_source = "declared_sequence"
            return meta

        if source == "max_content_date":
            d = _max_content_date(meta, cfg)
            if d:
                meta.snapshot_date, meta.snapshot_date_source = d, "max_content_date"
                meta.issues.append("SNAPSHOT_DATE_INFERRED_FROM_CONTENT")
                log.warning("%s: fecha de corte inferida del contenido (%s). "
                            "Preferible pasar --snapshot-date.", meta.name, d)
                return meta

    raise CriticalIngestionError(
        "UNRESOLVED_SNAPSHOT_DATE",
        f"No se pudo determinar la fecha del corte de {meta.name}. "
        f"Use --snapshot-date YYYY-MM-DD o declare el archivo en "
        f"config/pipeline.yml -> ingestion.declared_sequence")


def _as_date(value) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _max_content_date(meta: FileMetadata, cfg: Config) -> date | None:
    """max(date) del archivo, leyendo SOLO la columna date (projection pushdown)."""
    import duckdb

    formats = cfg.get("normalization.accepted_date_formats", [])
    cases = " ".join(
        f"WHEN regexp_matches(trim(CAST(date AS VARCHAR)), '{f['regex']}') "
        f"THEN try_strptime(trim(CAST(date AS VARCHAR)), '{f['strptime']}')::DATE"
        for f in formats)
    try:
        con = duckdb.connect()
        row = con.execute(
            f"SELECT max(CASE {cases} END) FROM read_parquet(?)", [str(meta.path)]).fetchone()
        con.close()
        return row[0] if row and row[0] else None
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo inferir la fecha del contenido de %s: %s", meta.name, exc)
        return None


# ------------------------------------------------------------ descubrimiento
def discover_files(cfg: Config, input_path: Path | None = None,
                   input_dir: Path | None = None,
                   cli_date: date | None = None) -> list[FileMetadata]:
    """Devuelve los archivos candidatos ORDENADOS de forma confiable.

    Orden: (order_hint declarado, snapshot_date, nombre). Determinista.
    """
    if input_path:
        candidates = [Path(input_path)]
    else:
        base = Path(input_dir) if input_dir else cfg.paths.raw
        if not base.exists():
            raise CriticalIngestionError("FILE_NOT_FOUND", f"No existe el directorio {base}")
        candidates = sorted(base.glob(cfg.get("ingestion.file_glob", "*.parquet")))

    if not candidates:
        raise CriticalIngestionError(
            "FILE_NOT_FOUND",
            f"No se encontraron archivos que coincidan con "
            f"{cfg.get('ingestion.file_glob')} en la ruta indicada")

    metas: list[FileMetadata] = []
    for path in candidates:
        meta = read_metadata(path, cfg)
        validate_schema(meta, cfg)
        resolve_snapshot_date(meta, cfg, cli_date if len(candidates) == 1 else None)
        metas.append(meta)
        log.info("Descubierto %s | filas=%s | sha256=%s… | corte=%s (%s)",
                 meta.name, f"{meta.row_count:,}", meta.sha256[:12],
                 meta.snapshot_date, meta.snapshot_date_source)

    metas.sort(key=lambda m: (m.order_hint or 0, m.snapshot_date or date.min, m.name))
    return metas
