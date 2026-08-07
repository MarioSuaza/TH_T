"""Carga y acceso a la configuracion del pipeline y al contrato de datos.

Principios:
- Fuente unica de verdad: config/pipeline.yml y config/data_contract.yml.
- Ninguna ruta absoluta: todo se resuelve contra la raiz del proyecto.
- Todo valor puede sobreescribirse por variable de entorno TYBA_<A>__<B>.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "TYBA_"
ENV_SEP = "__"


def project_root() -> Path:
    """Raiz del repositorio (directorio que contiene `config/`)."""
    env = os.environ.get("TYBA_PROJECT_ROOT")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parent.parent


def _coerce(raw: str) -> Any:
    low = raw.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none", ""):
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _apply_env_overrides(cfg: dict) -> dict:
    """TYBA_GUARDS__MAX_DELETED_PCT__FAIL=0.4 -> cfg['guards']['max_deleted_pct']['fail']."""
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(ENV_PREFIX) or env_key == "TYBA_PROJECT_ROOT":
            continue
        parts = [p.lower() for p in env_key[len(ENV_PREFIX):].split(ENV_SEP) if p]
        if not parts:
            continue
        node = cfg
        for part in parts[:-1]:
            if not isinstance(node.get(part), dict):
                node[part] = {}
            node = node[part]
        node[parts[-1]] = _coerce(env_val)
    return cfg


@dataclass(frozen=True)
class Paths:
    """Rutas efectivamente usadas por el pipeline.

    Las capas Silver y Gold NO son directorios: son tablas dentro de la base
    (ver docs/decisions.md ADR-003). Solo existen en disco la zona raw, los
    reportes y el archivo DuckDB.
    """

    root: Path
    raw: Path
    reports: Path
    database_dir: Path
    database_file: Path

    def ensure(self) -> None:
        for p in (self.raw, self.reports, self.database_dir):
            p.mkdir(parents=True, exist_ok=True)


class Config:
    """Vista de solo lectura sobre pipeline.yml + data_contract.yml."""

    def __init__(self, raw_cfg: dict, contract: dict, root: Path,
                 contract_path: Path, contract_hash: str):
        self._cfg = raw_cfg
        self._contract = contract
        self.root = root
        self.contract_path = contract_path
        self._contract_hash = contract_hash

    # ------------------------------------------------------------------ acceso
    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self._cfg
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    @property
    def raw(self) -> dict:
        return self._cfg

    @property
    def contract(self) -> dict:
        return self._contract

    @property
    def contract_version(self) -> str:
        """Version semantica declarada por el contrato cargado."""
        return str(self._contract.get("contract_version") or "unversioned")

    @property
    def contract_hash(self) -> str:
        """SHA-256 de los bytes exactos del YAML utilizado en esta ejecucion."""
        return self._contract_hash

    # ------------------------------------------------------------------ rutas
    def _path(self, value: str) -> Path:
        """Resuelve contra la raiz del proyecto. Una ruta absoluta se respeta tal
        cual, lo que permite sobreescribirla por variable de entorno (util para
        montar la base en un volumen distinto sin tocar el repositorio)."""
        p = Path(str(value)).expanduser()
        return p if p.is_absolute() else self.root / p

    @property
    def paths(self) -> Paths:
        p = self._cfg["paths"]
        db_file = self._path(p["database_file"])
        return Paths(
            root=self.root,
            raw=self._path(p["raw_dir"]),
            reports=self._path(p["reports_dir"]),
            database_dir=db_file.parent,
            database_file=db_file,
        )

    # -------------------------------------------------------------- contrato
    @property
    def columns(self) -> list[dict]:
        return self._contract["columns"]

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]

    @property
    def required_columns(self) -> list[str]:
        return [c["name"] for c in self.columns if c.get("required")]

    @property
    def business_key(self) -> list[str]:
        return list(self._contract["identity"]["business_key"])

    @property
    def mutable_attributes(self) -> list[str]:
        return list(self._contract["identity"]["mutable_attributes"])

    @property
    def identity(self) -> dict:
        return self._contract["identity"]

    @property
    def error_codes(self) -> dict:
        return self._contract["error_codes"]

    def column(self, name: str) -> dict:
        for c in self.columns:
            if c["name"] == name:
                return c
        raise KeyError(f"Columna {name!r} no declarada en el contrato de datos")

    def domain_values(self, name: str) -> list[str]:
        dom = self.column(name).get("domain") or {}
        return list(dom.get("observed_values") or dom.get("canonical_values") or [])

    @property
    def type_synonyms(self) -> dict[str, list[str]]:
        return self.column("type")["domain"]["type_synonyms"]

    # ------------------------------------------------------------- numericos
    @property
    def amount_precision(self) -> int:
        return int(self.get("normalization.amount.precision", 20))

    @property
    def amount_scale(self) -> int:
        return int(self.get("normalization.amount.scale", 2))

    @property
    def amount_sql_type(self) -> str:
        return f"DECIMAL({self.amount_precision},{self.amount_scale})"

    # ---------------------------------------------------------- serializacion
    @property
    def field_separator(self) -> str:
        return self.identity["serialization"]["field_separator"] or "\x1f"

    @property
    def null_token(self) -> str:
        return self.identity["serialization"]["null_token"] or "\x00NULL"

    @property
    def hash_version(self) -> str:
        return self.identity.get("hash_version", "v1")


@lru_cache(maxsize=4)
def load_config(config_path: str | None = None) -> Config:
    root = project_root()
    cfg_path = Path(config_path) if config_path else root / "config" / "pipeline.yml"
    if not cfg_path.is_absolute():
        cfg_path = root / cfg_path
    with cfg_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    cfg = _apply_env_overrides(cfg)

    contract_path = root / cfg["paths"]["contract_file"]
    contract_bytes = contract_path.read_bytes()
    contract = yaml.safe_load(contract_bytes.decode("utf-8")) or {}
    contract_hash = hashlib.sha256(contract_bytes).hexdigest()

    return Config(cfg, contract, root, contract_path, contract_hash)
