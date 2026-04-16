"""
phantom.testing — утилиты для тестирования миграций.

Использование в conftest.py проекта:

    from phantom.testing import phantom_config, phantom_pool, applied_migrations
    import pytest
    from pathlib import Path
    from phantom.config import Config

    @pytest.fixture(scope="session")
    def phantom_config():
        return Config(
            endpoint="grpc://localhost:2136",
            database="/local",
            auth_anonymous=True,
            migrations_dir=Path("migrations"),
            table_name="phantom_migrations_test",
        )
"""
from __future__ import annotations

import pytest
import ydb

from phantom.config import Config, load_config
from phantom.db import session_pool as _session_pool
from phantom.exceptions import MigrationApplyError, MigrationRollbackError
from phantom.loader import MigrationFile, discover_migrations, load_migration_module
from phantom.runner import run_down, run_up
from phantom.tracker import (
    ensure_table,
    get_applied_versions,
    record_applied,
    remove_applied,
)


def stairway_test(pool: ydb.SessionPool, config: Config) -> None:
    """
    Проверяет каждую миграцию по схеме: up → down → up.

    Для каждой миграции N в порядке возрастания:
      1. Применить миграции от 0 до N включительно.
      2. Откатить миграцию N.
      3. Снова применить миграцию N.

    После завершения все миграции остаются применёнными.
    Выбрасывает MigrationApplyError / MigrationRollbackError при сбое.
    """
    pool.retry_operation_sync(lambda session: ensure_table(session, config))
    all_migrations = discover_migrations(config.migrations_dir)

    for i, mf in enumerate(all_migrations):
        # Шаг 1: применить все миграции до i включительно (уже применённые пропустятся)
        applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
        run_up(pool, config, all_migrations[: i + 1], applied)

        # Шаг 2: откатить миграцию i
        applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
        run_down(pool, config, all_migrations[: i + 1], applied)

        # Шаг 3: снова применить миграцию i
        applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
        run_up(pool, config, all_migrations[: i + 1], applied)


# ---------------------------------------------------------------------------
# pytest-фикстуры
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def phantom_config() -> Config:  # type: ignore[return]
    """
    Базовая фикстура конфига. Переопределите в conftest.py вашего проекта:

        @pytest.fixture(scope="session")
        def phantom_config():
            return Config(endpoint=..., database=..., auth_anonymous=True, ...)
    """
    return load_config()


@pytest.fixture(scope="session")
def phantom_pool(phantom_config: Config):  # type: ignore[return]
    """Сессионный пул YDB на весь тестовый прогон."""
    with _session_pool(phantom_config) as pool:
        yield pool


@pytest.fixture()
def applied_migrations(phantom_pool: ydb.SessionPool, phantom_config: Config):
    """
    Накатывает все миграции перед тестом, откатывает в обратном порядке после.

    Использование:
        def test_something(applied_migrations):
            # все миграции применены
            ...
    """
    pool = phantom_pool
    config = phantom_config

    pool.retry_operation_sync(lambda s: ensure_table(s, config))
    migrations = discover_migrations(config.migrations_dir)
    applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
    run_up(pool, config, migrations, applied)

    yield

    for _ in migrations:
        applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
        if not applied:
            break
        run_down(pool, config, migrations, applied)


@pytest.fixture()
def migrated_db(phantom_pool: ydb.SessionPool, phantom_config: Config):
    """
    Накатывает все миграции и возвращает пул сессий. Без отката после теста.
    Удобно когда тест сам управляет состоянием БД.
    """
    pool = phantom_pool
    config = phantom_config

    pool.retry_operation_sync(lambda s: ensure_table(s, config))
    migrations = discover_migrations(config.migrations_dir)
    applied = pool.retry_operation_sync(lambda s: get_applied_versions(s, config))
    run_up(pool, config, migrations, applied)

    yield pool