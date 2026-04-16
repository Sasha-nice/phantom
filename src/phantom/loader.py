from __future__ import annotations

import hashlib
import importlib.util
import re
import types
from dataclasses import dataclass
from pathlib import Path

from phantom.exceptions import MigrationFileError

_MIGRATION_NAME_RE = re.compile(r"^(\d+)_(\w+)$")


@dataclass(frozen=True)
class MigrationFile:
    version: str       # например, "001_add_users"
    num: int           # числовой префикс для сортировки
    path: Path
    checksum: str      # SHA-256 hex содержимого файла


def discover_migrations(migrations_dir: Path) -> list[MigrationFile]:
    """
    Находит все файлы миграций в migrations_dir, сортирует по числовому префиксу.
    Raises MigrationFileError если директория не существует.
    """
    if not migrations_dir.exists():
        raise MigrationFileError(f"Директория миграций не найдена: {migrations_dir}")
    if not migrations_dir.is_dir():
        raise MigrationFileError(f"Не является директорией: {migrations_dir}")

    result: list[MigrationFile] = []
    for path in migrations_dir.glob("*.py"):
        stem = path.stem
        m = _MIGRATION_NAME_RE.match(stem)
        if not m:
            continue
        num = int(m.group(1))
        result.append(
            MigrationFile(
                version=stem,
                num=num,
                path=path,
                checksum=compute_checksum(path),
            )
        )

    result.sort(key=lambda mf: mf.num)
    return result


def load_migration_module(mf: MigrationFile) -> types.ModuleType:
    """
    Загружает файл миграции как Python-модуль.
    Raises MigrationFileError если отсутствует callable up().
    """
    spec = importlib.util.spec_from_file_location(f"phantom_migration_{mf.version}", mf.path)
    if spec is None or spec.loader is None:
        raise MigrationFileError(f"Не удалось загрузить модуль: {mf.path}")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        raise MigrationFileError(f"Ошибка при загрузке {mf.path}: {exc}") from exc

    if not callable(getattr(module, "up", None)):
        raise MigrationFileError(f"Функция up() не найдена в {mf.path}")

    return module


def compute_checksum(path: Path) -> str:
    """SHA-256 hex от содержимого файла."""
    return hashlib.sha256(path.read_bytes()).hexdigest()