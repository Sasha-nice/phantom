from __future__ import annotations

import sys
from pathlib import Path

import click

from phantom.config import Config, load_config
from phantom.db import session_pool
from phantom.exceptions import PhantomError
from phantom.loader import discover_migrations
from phantom.runner import run_down, run_up
from phantom.tracker import check_checksums, ensure_table, get_applied_versions

_MIGRATION_TEMPLATE = '''\
"""Миграция: {version}"""


def up(session) -> None:
    """Применить миграцию. Должна быть идемпотентной."""
    # session.execute_scheme("""
    #     CREATE TABLE `my_table` (
    #         id Uint64 NOT NULL,
    #         PRIMARY KEY (id)
    #     );
    # """)
    raise NotImplementedError


def down(session) -> None:
    """Откатить миграцию."""
    # session.execute_scheme("DROP TABLE `my_table`;")
    raise NotImplementedError
'''


@click.group()
@click.option("--config", "-c", "config_file", type=click.Path(), default=None,
              help="Путь к phantom.yml")
@click.option("--migrations-dir", "-m", type=click.Path(), default=None,
              help="Директория с миграциями")
@click.pass_context
def main(ctx: click.Context, config_file: str | None, migrations_dir: str | None) -> None:
    """phantom — инструмент миграций для YDB."""
    ctx.ensure_object(dict)
    try:
        cfg = load_config(Path(config_file) if config_file else None)
    except PhantomError as exc:
        # Для команд, не требующих подключения к YDB (create_migration),
        # конфиг может быть не задан — откладываем ошибку до момента использования.
        cfg = None  # type: ignore[assignment]
        ctx.obj["config_error"] = str(exc)
    else:
        ctx.obj["config_error"] = None

    if cfg is not None and migrations_dir:
        cfg.migrations_dir = Path(migrations_dir)
    elif migrations_dir:
        # config не загружен, но migrations_dir задан явно
        ctx.obj["migrations_dir_override"] = Path(migrations_dir)

    ctx.obj["config"] = cfg


@main.command("create_migration")
@click.argument("name")
@click.pass_context
def create_migration(ctx: click.Context, name: str) -> None:
    """Создать новый файл миграции."""
    cfg: Config | None = ctx.obj.get("config")

    if cfg is not None:
        migrations_dir = cfg.migrations_dir
    elif "migrations_dir_override" in ctx.obj:
        migrations_dir = ctx.obj["migrations_dir_override"]
    else:
        migrations_dir = Path("migrations")

    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Определяем следующий номер
    existing = list(migrations_dir.glob("*.py"))
    nums = []
    for p in existing:
        parts = p.stem.split("_", 1)
        if parts[0].isdigit():
            nums.append(int(parts[0]))
    next_num = (max(nums) + 1) if nums else 1
    padded = str(next_num).zfill(3)
    version = f"{padded}_{name}"
    file_path = migrations_dir / f"{version}.py"

    file_path.write_text(_MIGRATION_TEMPLATE.format(version=version))
    click.echo(f"Создан файл миграции: {file_path}")


@main.command("up")
@click.option("--dry-run", is_flag=True, help="Показать что будет применено, не применяя")
@click.pass_context
def cmd_up(ctx: click.Context, dry_run: bool) -> None:
    """Применить все непримененные миграции."""
    cfg = _require_config(ctx)

    try:
        with session_pool(cfg) as pool:
            pool.retry_operation_sync(lambda session: ensure_table(session, cfg))
            applied = pool.retry_operation_sync(lambda session: get_applied_versions(session, cfg))
            all_migrations = discover_migrations(cfg.migrations_dir)

            mismatches = pool.retry_operation_sync(
                lambda session: check_checksums(session, cfg, all_migrations)
            )
            for v in mismatches:
                click.echo(
                    f"Предупреждение: файл применённой миграции {v} изменился на диске.",
                    err=True,
                )

            run_up(pool, cfg, all_migrations, applied, dry_run=dry_run)
    except PhantomError as exc:
        click.echo(f"Ошибка: {exc}", err=True)
        sys.exit(1)


@main.command("down")
@click.option("--dry-run", is_flag=True, help="Показать что будет откачено, не откатывая")
@click.pass_context
def cmd_down(ctx: click.Context, dry_run: bool) -> None:
    """Откатить последнюю применённую миграцию."""
    cfg = _require_config(ctx)

    try:
        with session_pool(cfg) as pool:
            pool.retry_operation_sync(lambda session: ensure_table(session, cfg))
            applied = pool.retry_operation_sync(lambda session: get_applied_versions(session, cfg))
            all_migrations = discover_migrations(cfg.migrations_dir)
            run_down(pool, cfg, all_migrations, applied, dry_run=dry_run)
    except PhantomError as exc:
        click.echo(f"Ошибка: {exc}", err=True)
        sys.exit(1)


def _require_config(ctx: click.Context) -> Config:
    cfg: Config | None = ctx.obj.get("config")
    if cfg is None:
        error = ctx.obj.get("config_error", "Конфиг не загружен")
        click.echo(f"Ошибка: {error}", err=True)
        sys.exit(1)
    return cfg