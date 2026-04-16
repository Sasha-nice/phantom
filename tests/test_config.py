import pytest
from pathlib import Path

from phantom.config import load_config, Config
from phantom.exceptions import ConfigError


def test_load_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("YDB_ENDPOINT", "grpc://localhost:2136")
    monkeypatch.setenv("YDB_DATABASE", "/local")
    cfg = load_config(tmp_path / "nonexistent.yml")
    assert cfg.endpoint == "grpc://localhost:2136"
    assert cfg.database == "/local"


def test_env_overrides_yaml(monkeypatch, tmp_path):
    yml = tmp_path / "phantom.yml"
    yml.write_text("endpoint: grpc://from-yaml:2136\ndatabase: /yaml-db\n")
    monkeypatch.setenv("YDB_ENDPOINT", "grpc://from-env:2136")
    monkeypatch.delenv("YDB_DATABASE", raising=False)
    cfg = load_config(yml)
    assert cfg.endpoint == "grpc://from-env:2136"
    assert cfg.database == "/yaml-db"


def test_missing_endpoint_raises(monkeypatch, tmp_path):
    monkeypatch.delenv("YDB_ENDPOINT", raising=False)
    monkeypatch.setenv("YDB_DATABASE", "/local")
    with pytest.raises(ConfigError, match="endpoint"):
        load_config(tmp_path / "nonexistent.yml")


def test_missing_database_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("YDB_ENDPOINT", "grpc://localhost:2136")
    monkeypatch.delenv("YDB_DATABASE", raising=False)
    with pytest.raises(ConfigError, match="database"):
        load_config(tmp_path / "nonexistent.yml")


def test_auth_token_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("YDB_ENDPOINT", "grpc://localhost:2136")
    monkeypatch.setenv("YDB_DATABASE", "/local")
    monkeypatch.setenv("YDB_TOKEN", "mytoken")
    cfg = load_config(tmp_path / "nonexistent.yml")
    assert cfg.auth_token == "mytoken"


def test_migrations_dir_default(monkeypatch, tmp_path):
    monkeypatch.setenv("YDB_ENDPOINT", "grpc://localhost:2136")
    monkeypatch.setenv("YDB_DATABASE", "/local")
    monkeypatch.delenv("PHANTOM_MIGRATIONS_DIR", raising=False)
    cfg = load_config(tmp_path / "nonexistent.yml")
    assert cfg.migrations_dir == Path("migrations")


def test_table_name_from_yaml(monkeypatch, tmp_path):
    yml = tmp_path / "phantom.yml"
    yml.write_text(
        "endpoint: grpc://localhost:2136\ndatabase: /local\ntable_name: custom_table\n"
    )
    monkeypatch.delenv("YDB_ENDPOINT", raising=False)
    monkeypatch.delenv("YDB_DATABASE", raising=False)
    monkeypatch.delenv("PHANTOM_TABLE_NAME", raising=False)
    cfg = load_config(yml)
    assert cfg.table_name == "custom_table"