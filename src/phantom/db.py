from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

import ydb

from phantom.config import Config
from phantom.exceptions import DatabaseError


def build_driver(config: Config) -> ydb.Driver:
    if config.auth_token:
        credentials = ydb.AccessTokenCredentials(config.auth_token)
    elif config.auth_sa_key_file:
        credentials = ydb.iam.ServiceAccountCredentials.from_file(config.auth_sa_key_file)
    else:
        credentials = ydb.AnonymousCredentials()

    driver_config = ydb.DriverConfig(
        endpoint=config.endpoint,
        database=config.database,
        credentials=credentials,
    )
    return ydb.Driver(driver_config=driver_config)


@contextmanager
def session_pool(config: Config) -> Generator[ydb.SessionPool, None, None]:
    """Контекстный менеджер: создаёт driver, ждёт готовности, возвращает SessionPool."""
    driver = build_driver(config)
    try:
        driver.wait(timeout=10, fail_fast=True)
    except Exception as exc:
        driver.stop()
        raise DatabaseError(f"Не удалось подключиться к YDB ({config.endpoint}): {exc}") from exc

    pool = ydb.SessionPool(driver)
    try:
        yield pool
    finally:
        pool.stop()
        driver.stop()