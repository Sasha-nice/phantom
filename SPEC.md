# phantom — спецификация

## Описание

`phantom` — инструмент версионных миграций для YDB, аналог Alembic/yoyo-migrations.
Устанавливается как Python-пакет, предоставляет CLI-команды и библиотечный API для тестов.

---

## Команды CLI

```bash
phantom create_migration <name>   # создать файл миграции
phantom up [--dry-run]            # применить все непримененные миграции
phantom down [--dry-run]          # откатить последнюю миграцию
```

Глобальные опции (перед командой):
```bash
phantom --config path/to/phantom.yml up
phantom --migrations-dir custom/path up
```

---

## Конфиг: `phantom.yml`

Ищется вверх по дереву директорий от CWD (как `.gitignore`).
Переменные окружения перекрывают значения из файла.

```yaml
endpoint: "grpcs://ydb.example.com:2135"   # → YDB_ENDPOINT
database: "/ru-central1/folder/mydb"        # → YDB_DATABASE
auth:
  token: "..."                               # → YDB_TOKEN
  # service_account_key: "/path/to/key.json" # → YDB_SA_KEY_FILE
  # anonymous: true
migrations_dir: "migrations"                 # → PHANTOM_MIGRATIONS_DIR (default: migrations)
table_name: "phantom_migrations"             # → PHANTOM_TABLE_NAME (default: phantom_migrations)
```

---

## Файл миграции

Создаётся командой `phantom create_migration <name>`.
Имя файла: `<NNN>_<name>.py`, где `NNN` — следующий свободный порядковый номер (с ведущими нулями).

```python
"""Миграция: 001_add_users"""


def up(session) -> None:
    """Применить миграцию. Должна быть идемпотентной."""
    session.execute_scheme("""
        CREATE TABLE `users` (
            id Uint64 NOT NULL,
            PRIMARY KEY (id)
        );
    """)


def down(session) -> None:
    """Откатить миграцию."""
    session.execute_scheme("DROP TABLE `users`;")
```

`session` — объект `ydb.Session`:
- DDL: `session.execute_scheme(yql)`
- DML: `session.transaction().execute(yql, commit_tx=True)`

---

## Tracking-таблица в YDB

Создаётся автоматически при первом запуске `phantom up` / `phantom down`.

```sql
CREATE TABLE IF NOT EXISTS `phantom_migrations` (
    version     Utf8     NOT NULL,   -- "001_add_users"
    applied_at  Datetime NOT NULL,
    checksum    Utf8     NOT NULL,   -- SHA-256 содержимого файла
    PRIMARY KEY (version)
);
```

---

## Алгоритм `phantom up`

1. Найти все `*.py` в `migrations_dir`, отфильтровать по паттерну `^\d{3,}_\w+$`, отсортировать по числовому префиксу.
2. Получить список применённых версий из `phantom_migrations`.
3. Предупредить, если checksum применённых файлов изменился на диске.
4. Предупредить (или завершить с ошибкой при `--strict`), если ожидающая миграция имеет номер меньше уже применённых.
5. Для каждой непримененной миграции по порядку:
   a. Вызвать `up(session)`.
   b. При успехе — записать в `phantom_migrations`.
   c. При ошибке — остановиться, не продолжать.

---

## Поведение при ошибках

| Ситуация | Поведение |
|---|---|
| `up()` упала на миграции N | N не записывается; миграции до N остаются применёнными |
| Изменён файл применённой миграции | Предупреждение (ошибка при `--strict`) |
| Ожидающая миграция с меньшим номером | Предупреждение + требует `--force` |
| Файл применённой миграции удалён с диска | Предупреждение |
| YDB недоступна | `DatabaseError` → понятное сообщение + exit 1 |

---

## Тестовые утилиты (`phantom.testing`)

### `stairway_test(pool, config)`

Для каждой миграции N по порядку: `up` → `down` → `up`.
Гарантирует корректность `down()` и идемпотентность `up()`.

### pytest-фикстуры

| Фикстура | Scope | Поведение |
|---|---|---|
| `phantom_config` | session | Базовый конфиг; переопределяется в `conftest.py` |
| `phantom_pool` | session | Пул сессий YDB на весь прогон |
| `applied_migrations` | function | up всех миграций до теста, down после |
| `migrated_db` | function | up всех миграций, возвращает пул (без отката) |

Пример переопределения в проекте пользователя:

```python
# tests/conftest.py
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
```

Пример теста:

```python
# tests/test_migrations.py
from phantom.testing import stairway_test

def test_stairway(phantom_pool, phantom_config):
    stairway_test(phantom_pool, phantom_config)

def test_schema_after_migrations(applied_migrations, phantom_pool):
    result = phantom_pool.retry_operation_sync(
        lambda s: s.transaction().execute("SELECT 1 FROM `users` LIMIT 1;", commit_tx=True)
    )
    assert result is not None
```

---

## Структура пакета

```
src/phantom/
├── __init__.py      # версия пакета
├── cli.py           # click-команды
├── config.py        # загрузка конфига
├── db.py            # YDB driver/session pool
├── tracker.py       # CRUD phantom_migrations
├── loader.py        # поиск и загрузка файлов миграций
├── runner.py        # оркестрация up/down
├── exceptions.py    # иерархия ошибок
└── testing.py       # stairway_test + pytest-фикстуры
```

Иерархия исключений:
```
PhantomError
├── ConfigError
├── MigrationFileError
├── MigrationApplyError
├── MigrationRollbackError
├── ChecksumMismatchError
└── DatabaseError
```