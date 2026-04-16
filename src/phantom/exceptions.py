class PhantomError(Exception):
    """Базовый класс для всех ошибок phantom."""


class ConfigError(PhantomError):
    """Некорректный или отсутствующий конфиг."""


class MigrationFileError(PhantomError):
    """Ошибка в файле миграции (отсутствует up, неверное имя и т.д.)."""


class MigrationApplyError(PhantomError):
    """Ошибка при выполнении up()."""


class MigrationRollbackError(PhantomError):
    """Ошибка при выполнении down()."""


class ChecksumMismatchError(PhantomError):
    """Файл уже применённой миграции был изменён на диске."""


class DatabaseError(PhantomError):
    """Ошибка при работе с YDB (обёртка над ydb.Error)."""