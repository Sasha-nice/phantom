from __future__ import annotations

import click
import ydb

from phantom.config import Config
from phantom.exceptions import MigrationApplyError, MigrationRollbackError
from phantom.loader import MigrationFile, load_migration_module
from phantom.tracker import record_applied, remove_applied


def run_up(
    pool: ydb.SessionPool,
    config: Config,
    all_migrations: list[MigrationFile],
    applied_versions: list[str],
    dry_run: bool = False,
) -> None:
    applied_set = set(applied_versions)
    pending = [m for m in all_migrations if m.version not in applied_set]

    if not pending:
        click.echo("Нет миграций для применения.")
        return

    # Предупреждение о неупорядоченности
    max_applied_num = max((m.num for m in all_migrations if m.version in applied_set), default=-1)
    for mf in pending:
        if mf.num < max_applied_num:
            click.echo(
                f"Предупреждение: миграция {mf.version} ожидает применения, "
                f"но миграции с большим номером уже применены.",
                err=True,
            )

    for mf in pending:
        if dry_run:
            click.echo(f"Would apply: {mf.version}")
            continue

        click.echo(f"Применяю: {mf.version} ...", nl=False)
        module = load_migration_module(mf)
        try:
            pool.retry_operation_sync(lambda session, m=mf, mod=module: mod.up(session))
        except Exception as exc:
            click.echo(" ОШИБКА")
            raise MigrationApplyError(
                f"Ошибка при применении {mf.version}: {exc}"
            ) from exc

        pool.retry_operation_sync(
            lambda session, m=mf: record_applied(session, config, m.version, m.checksum)
        )
        click.echo(" OK")


def run_down(
    pool: ydb.SessionPool,
    config: Config,
    all_migrations: list[MigrationFile],
    applied_versions: list[str],
    dry_run: bool = False,
) -> None:
    if not applied_versions:
        click.echo("Нет применённых миграций для отката.")
        return

    last_version = applied_versions[-1]
    mf = next((m for m in all_migrations if m.version == last_version), None)

    if mf is None:
        raise MigrationRollbackError(
            f"Файл миграции {last_version} не найден на диске. Откат невозможен."
        )

    module = load_migration_module(mf)
    if not callable(getattr(module, "down", None)):
        raise MigrationRollbackError(
            f"Функция down() не найдена в {mf.path}. Откат невозможен."
        )

    if dry_run:
        click.echo(f"Would roll back: {last_version}")
        return

    click.echo(f"Откатываю: {last_version} ...", nl=False)
    try:
        pool.retry_operation_sync(lambda session, mod=module: mod.down(session))
    except Exception as exc:
        click.echo(" ОШИБКА")
        raise MigrationRollbackError(
            f"Ошибка при откате {last_version}: {exc}"
        ) from exc

    pool.retry_operation_sync(
        lambda session, v=last_version: remove_applied(session, config, v)
    )
    click.echo(" OK")