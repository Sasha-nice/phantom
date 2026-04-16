from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from phantom.exceptions import ConfigError


@dataclass
class Config:
    endpoint: str
    database: str
    auth_token: str | None = None
    auth_sa_key_file: str | None = None
    auth_anonymous: bool = False
    migrations_dir: Path = field(default_factory=lambda: Path("migrations"))
    table_name: str = "phantom_migrations"


def load_config(config_file: Path | None = None) -> Config:
    """
    Загружает конфиг из phantom.yml (ищется вверх от CWD) и перекрывает env-переменными.
    Raises ConfigError если endpoint или database не заданы.
    """
    raw: dict = {}

    yml_path = config_file or _find_config_file()
    if yml_path and yml_path.exists():
        with yml_path.open() as f:
            raw = yaml.safe_load(f) or {}

    auth = raw.get("auth") or {}

    endpoint = os.environ.get("YDB_ENDPOINT") or raw.get("endpoint") or ""
    database = os.environ.get("YDB_DATABASE") or raw.get("database") or ""

    if not endpoint:
        raise ConfigError(
            "YDB endpoint не задан. Укажите YDB_ENDPOINT или поле endpoint в phantom.yml"
        )
    if not database:
        raise ConfigError(
            "YDB database не задан. Укажите YDB_DATABASE или поле database в phantom.yml"
        )

    token = os.environ.get("YDB_TOKEN") or auth.get("token")
    sa_key = os.environ.get("YDB_SA_KEY_FILE") or auth.get("service_account_key")
    anonymous = bool(auth.get("anonymous", False))

    migrations_dir_str = (
        os.environ.get("PHANTOM_MIGRATIONS_DIR")
        or raw.get("migrations_dir")
        or "migrations"
    )
    table_name = (
        os.environ.get("PHANTOM_TABLE_NAME")
        or raw.get("table_name")
        or "phantom_migrations"
    )

    return Config(
        endpoint=endpoint,
        database=database,
        auth_token=token,
        auth_sa_key_file=sa_key,
        auth_anonymous=anonymous,
        migrations_dir=Path(migrations_dir_str),
        table_name=table_name,
    )


def _find_config_file() -> Path | None:
    """Ищет phantom.yml вверх по дереву директорий от CWD."""
    current = Path.cwd()
    while True:
        candidate = current / "phantom.yml"
        if candidate.exists():
            return candidate
        parent = current.parent
        if parent == current:
            return None
        current = parent