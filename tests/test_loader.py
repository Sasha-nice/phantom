import pytest
from pathlib import Path

from phantom.loader import discover_migrations, load_migration_module, compute_checksum
from phantom.exceptions import MigrationFileError


def write_migration(dir: Path, name: str, content: str = "") -> Path:
    path = dir / name
    path.write_text(content or f'def up(session): pass\ndef down(session): pass\n')
    return path


def test_discover_finds_migrations(tmp_path):
    write_migration(tmp_path, "001_init.py")
    write_migration(tmp_path, "002_add_users.py")
    migrations = discover_migrations(tmp_path)
    assert [m.version for m in migrations] == ["001_init", "002_add_users"]


def test_discover_sorts_by_number(tmp_path):
    write_migration(tmp_path, "010_late.py")
    write_migration(tmp_path, "002_early.py")
    migrations = discover_migrations(tmp_path)
    assert migrations[0].version == "002_early"
    assert migrations[1].version == "010_late"


def test_discover_skips_invalid_names(tmp_path):
    write_migration(tmp_path, "001_valid.py")
    (tmp_path / "no_prefix.py").write_text("def up(session): pass\n")
    (tmp_path / "__init__.py").write_text("")
    migrations = discover_migrations(tmp_path)
    assert len(migrations) == 1
    assert migrations[0].version == "001_valid"


def test_discover_missing_dir_raises():
    with pytest.raises(MigrationFileError, match="не найдена"):
        discover_migrations(Path("/nonexistent/path"))


def test_load_migration_module(tmp_path):
    path = write_migration(tmp_path, "001_test.py", "def up(session): return 42\n")
    from phantom.loader import MigrationFile, compute_checksum
    mf = MigrationFile(version="001_test", num=1, path=path, checksum=compute_checksum(path))
    module = load_migration_module(mf)
    assert callable(module.up)


def test_load_migration_module_missing_up(tmp_path):
    path = tmp_path / "001_bad.py"
    path.write_text("def down(session): pass\n")
    from phantom.loader import MigrationFile, compute_checksum
    mf = MigrationFile(version="001_bad", num=1, path=path, checksum=compute_checksum(path))
    with pytest.raises(MigrationFileError, match="up\\(\\)"):
        load_migration_module(mf)


def test_compute_checksum_deterministic(tmp_path):
    path = tmp_path / "file.py"
    path.write_text("hello")
    c1 = compute_checksum(path)
    c2 = compute_checksum(path)
    assert c1 == c2
    assert len(c1) == 64  # SHA-256 hex


def test_compute_checksum_changes_on_edit(tmp_path):
    path = tmp_path / "file.py"
    path.write_text("hello")
    c1 = compute_checksum(path)
    path.write_text("world")
    c2 = compute_checksum(path)
    assert c1 != c2