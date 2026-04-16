# phantom — план реализации

## Стек

- Python 3.12
- uv (менеджер пакетов и окружений)
- `ydb` SDK (официальный)
- `click` (CLI)
- `pyyaml` (конфиг)
- `hatchling` (build backend)
- `pytest`, `pytest-mock`, `ruff`, `mypy` (dev)

---

## Структура проекта

```
phantom/
├── pyproject.toml
├── uv.lock
├── SPEC.md
├── PLAN.md
├── migrations/
│   └── .gitkeep
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_loader.py
│   ├── test_runner.py
│   └── test_cli.py
└── src/
    └── phantom/
        ├── __init__.py
        ├── cli.py
        ├── config.py
        ├── db.py
        ├── tracker.py
        ├── loader.py
        ├── runner.py
        ├── exceptions.py
        └── testing.py
```

---

## Порядок реализации

### 1. Scaffold пакета
- [ ] `pyproject.toml` с зависимостями и entry point `phantom = "phantom.cli:main"`
- [ ] `src/phantom/__init__.py` (версия `0.1.0`)
- [ ] `migrations/.gitkeep`
- [ ] Инициализация uv: `uv add ydb click pyyaml && uv add --dev pytest pytest-mock ruff mypy`
- [ ] `uv pip install -e .` — убедиться, что `phantom --help` работает

### 2. `exceptions.py`
- [ ] Иерархия исключений: `PhantomError` и все дочерние

### 3. `config.py`
- [ ] Датакласс `Config`
- [ ] `load_config(config_file)` — YAML + env vars, поиск вверх от CWD
- [ ] Юнит-тесты (`tests/test_config.py`)

### 4. `loader.py`
- [ ] Датакласс `MigrationFile`
- [ ] `discover_migrations(dir)` — glob, фильтрация, сортировка по числовому префиксу
- [ ] `load_migration_module(mf)` — importlib, валидация callable `up`
- [ ] `compute_checksum(path)` — SHA-256 hex
- [ ] Юнит-тесты (`tests/test_loader.py`)

### 5. `cli.py` — команда `create_migration`
- [ ] Click-группа `main` с опциями `--config`, `--migrations-dir`
- [ ] Команда `create_migration <name>`: вычислить следующий номер, записать шаблон
- [ ] Тест через `CliRunner` (`tests/test_cli.py`)

### 6. `db.py`
- [ ] `build_driver(config)` — выбор credentials (token / SA key / anonymous)
- [ ] `session_pool(config)` — контекстный менеджер: driver.wait → SessionPool → yield → close

### 7. `tracker.py`
- [ ] `ensure_table(session, config)` — DDL через `execute_scheme()`
- [ ] `get_applied_versions(session, config) -> list[str]`
- [ ] `record_applied(session, config, version, checksum)`
- [ ] `remove_applied(session, config, version)`
- [ ] `check_checksums(session, config, migrations) -> list[str]`

### 8. `runner.py`
- [ ] `run_up(pool, config, all_migrations, applied_versions, dry_run)`
- [ ] `run_down(pool, config, all_migrations, applied_versions, dry_run)`
- [ ] Юнит-тесты с mock SessionPool (`tests/test_runner.py`)

### 9. `cli.py` — команды `up` / `down`
- [ ] `phantom up [--dry-run]`
- [ ] `phantom down [--dry-run]`

### 10. `testing.py`
- [ ] `stairway_test(pool, config)` — up → down → up для каждой миграции
- [ ] pytest-фикстуры: `phantom_config`, `phantom_pool`, `applied_migrations`, `migrated_db`

---

## pyproject.toml (итоговый)

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "phantom-migrations"
version = "0.1.0"
description = "Инструмент миграций для YDB, аналог Alembic"
readme = "README.md"
requires-python = ">=3.12"
license = { text = "MIT" }
dependencies = [
    "ydb>=3.28.0",
    "click>=8.1",
    "pyyaml>=6.0",
]

[project.scripts]
phantom = "phantom.cli:main"

[tool.hatch.build.targets.wheel]
packages = ["src/phantom"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-mock>=3.12",
    "ruff>=0.4",
    "mypy>=1.9",
]
```

---

## Проверка после реализации

```bash
# Установка
uv pip install -e .

# Создать миграцию
phantom create_migration add_users
# → migrations/001_add_users.py

# Применить
YDB_ENDPOINT=grpc://localhost:2136 YDB_DATABASE=/local phantom up
# → Applied: 001_add_users

# Повтор — no-op
phantom up
# → Nothing to apply

# Откат
phantom down
# → Rolled back: 001_add_users

# Dry run
phantom up --dry-run
# → Would apply: 001_add_users

# Запуск тестов
uv run pytest tests/
```