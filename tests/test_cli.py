import pytest
from pathlib import Path
from click.testing import CliRunner

from phantom.cli import main


def test_create_migration_creates_file(tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, ["--migrations-dir", str(tmp_path), "create_migration", "add_users"])
    assert result.exit_code == 0, result.output
    files = list(tmp_path.glob("*.py"))
    assert len(files) == 1
    assert files[0].name == "001_add_users.py"


def test_create_migration_increments_number(tmp_path):
    runner = CliRunner()
    runner.invoke(main, ["--migrations-dir", str(tmp_path), "create_migration", "first"])
    runner.invoke(main, ["--migrations-dir", str(tmp_path), "create_migration", "second"])
    files = sorted(tmp_path.glob("*.py"))
    assert files[0].name == "001_first.py"
    assert files[1].name == "002_second.py"


def test_create_migration_template_has_up_and_down(tmp_path):
    runner = CliRunner()
    runner.invoke(main, ["--migrations-dir", str(tmp_path), "create_migration", "test"])
    content = (tmp_path / "001_test.py").read_text()
    assert "def up(session)" in content
    assert "def down(session)" in content


def test_up_fails_without_config(tmp_path, monkeypatch):
    monkeypatch.delenv("YDB_ENDPOINT", raising=False)
    monkeypatch.delenv("YDB_DATABASE", raising=False)
    monkeypatch.chdir(tmp_path)  # нет phantom.yml
    runner = CliRunner()
    result = runner.invoke(main, ["up"])
    assert result.exit_code == 1
    assert "Ошибка" in result.output


def test_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "phantom" in result.output.lower()