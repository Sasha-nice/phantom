from __future__ import annotations

import ydb

from phantom.config import Config
from phantom.loader import MigrationFile


def ensure_table(session: ydb.Session, config: Config) -> None:
    """Создаёт tracking-таблицу если она не существует."""
    session.execute_scheme(f"""
        CREATE TABLE IF NOT EXISTS `{config.table_name}` (
            version     Utf8     NOT NULL,
            applied_at  Datetime NOT NULL,
            checksum    Utf8     NOT NULL,
            PRIMARY KEY (version)
        );
    """)


def get_applied_versions(session: ydb.Session, config: Config) -> list[str]:
    """Возвращает список применённых версий, отсортированных по возрастанию."""
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"SELECT version FROM `{config.table_name}` ORDER BY version ASC;",
        commit_tx=True,
    )
    return [row.version for row in result_sets[0].rows]


def record_applied(
    session: ydb.Session, config: Config, version: str, checksum: str
) -> None:
    """Записывает применённую миграцию в tracking-таблицу."""
    session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        UPSERT INTO `{config.table_name}` (version, applied_at, checksum)
        VALUES ("{version}", CurrentUtcDatetime(), "{checksum}");
        """,
        commit_tx=True,
    )


def remove_applied(session: ydb.Session, config: Config, version: str) -> None:
    """Удаляет запись об отменённой миграции."""
    session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        DELETE FROM `{config.table_name}` WHERE version = "{version}";
        """,
        commit_tx=True,
    )


def check_checksums(
    session: ydb.Session, config: Config, migrations: list[MigrationFile]
) -> list[str]:
    """
    Возвращает список версий уже применённых миграций, чьи файлы изменились на диске.
    """
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"SELECT version, checksum FROM `{config.table_name}`;",
        commit_tx=True,
    )
    stored: dict[str, str] = {row.version: row.checksum for row in result_sets[0].rows}
    disk: dict[str, str] = {mf.version: mf.checksum for mf in migrations}

    mismatches = []
    for version, stored_checksum in stored.items():
        if version in disk and disk[version] != stored_checksum:
            mismatches.append(version)
    return mismatches