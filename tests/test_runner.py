import pytest
from pathlib import Path
from unittest.mock import MagicMock, call

from phantom.config import Config
from phantom.exceptions import MigrationApplyError, MigrationRollbackError
from phantom.loader import MigrationFile, compute_checksum
from phantom.runner import run_up, run_down


def make_config(tmp_path: Path) -> Config:
    return Config(
        endpoint="grpc://localhost:2136",
        database="/local",
        auth_anonymous=True,
        migrations_dir=tmp_path,
        table_name="phantom_migrations_test",
    )


def make_migration(tmp_path: Path, name: str, content: str = "") -> MigrationFile:
    path = tmp_path / name
    body = content or "def up(session): pass\ndef down(session): pass\n"
    path.write_text(body)
    parts = path.stem.split("_", 1)
    return MigrationFile(
        version=path.stem,
        num=int(parts[0]),
        path=path,
        checksum=compute_checksum(path),
    )


def make_pool():
    pool = MagicMock()
    pool.retry_operation_sync.side_effect = lambda fn: fn(MagicMock())
    return pool


def test_run_up_applies_pending(tmp_path):
    config = make_config(tmp_path)
    m1 = make_migration(tmp_path, "001_init.py")
    m2 = make_migration(tmp_path, "002_users.py")
    pool = make_pool()

    run_up(pool, config, [m1, m2], applied_versions=["001_init"])

    # retry_operation_sync вызывается для up() и record_applied() только для m2
    assert pool.retry_operation_sync.call_count == 2


def test_run_up_nothing_to_apply(tmp_path, capsys):
    config = make_config(tmp_path)
    m1 = make_migration(tmp_path, "001_init.py")
    pool = make_pool()

    run_up(pool, config, [m1], applied_versions=["001_init"])

    captured = capsys.readouterr()
    assert "Нет миграций" in captured.out
    pool.retry_operation_sync.assert_not_called()


def test_run_up_stops_on_error(tmp_path):
    config = make_config(tmp_path)
    m1 = make_migration(tmp_path, "001_init.py", "def up(s): raise RuntimeError('boom')\n")
    pool = make_pool()

    with pytest.raises(MigrationApplyError, match="001_init"):
        run_up(pool, config, [m1], applied_versions=[])


def test_run_down_rolls_back_last(tmp_path):
    config = make_config(tmp_path)
    m1 = make_migration(tmp_path, "001_init.py")
    m2 = make_migration(tmp_path, "002_users.py")
    pool = make_pool()

    run_down(pool, config, [m1, m2], applied_versions=["001_init", "002_users"])

    # retry_operation_sync вызывается для down() и remove_applied() для m2
    assert pool.retry_operation_sync.call_count == 2


def test_run_down_nothing_applied(tmp_path, capsys):
    config = make_config(tmp_path)
    pool = make_pool()

    run_down(pool, config, [], applied_versions=[])

    captured = capsys.readouterr()
    assert "Нет применённых" in captured.out


def test_run_down_raises_when_file_missing(tmp_path):
    config = make_config(tmp_path)
    pool = make_pool()
    from phantom.exceptions import MigrationRollbackError

    with pytest.raises(MigrationRollbackError, match="не найден"):
        run_down(pool, config, [], applied_versions=["001_missing"])