"""
cache.py
--------
Простой кэш с TTL (время жизни записи) для данных, которые не нужно
запрашивать из БД при каждом обращении: список свободных игр, уровни игроков.

По умолчанию используется in-memory словарь (потокобезопасный, с блокировкой).
Если в окружении задан REDIS_URL и установлен пакет redis — автоматически
используется Redis, что позволяет расшарить кэш между несколькими процессами
(например web + bot-worker). Никакой код вне этого модуля не должен знать,
какой backend используется — это и есть смысл абстракции cache-aside.
"""

import json
import os
import threading
import time
from typing import Any, Callable, Optional

REDIS_URL = os.getenv("REDIS_URL")

_redis_client = None
if REDIS_URL:
    try:
        import redis  # type: ignore

        _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        _redis_client.ping()
    except Exception:
        # Redis недоступен или не установлен — просто работаем в in-memory режиме.
        _redis_client = None


class _InMemoryCache:
    """Простой TTL-кэш на словаре. Подходит для одного процесса."""

    def __init__(self):
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at is not None and time.monotonic() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        expires_at = time.monotonic() + ttl if ttl else None
        with self._lock:
            self._store[key] = (expires_at, value)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def delete_prefix(self, prefix: str) -> None:
        with self._lock:
            for key in list(self._store.keys()):
                if key.startswith(prefix):
                    del self._store[key]

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


class _RedisCache:
    """Обёртка над redis-py с тем же интерфейсом, что и у in-memory кэша."""

    def __init__(self, client):
        self._client = client

    def get(self, key: str) -> Any:
        raw = self._client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return raw

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        payload = json.dumps(value, default=str)
        if ttl:
            self._client.set(key, payload, ex=int(ttl))
        else:
            self._client.set(key, payload)

    def delete(self, key: str) -> None:
        self._client.delete(key)

    def delete_prefix(self, prefix: str) -> None:
        for key in self._client.scan_iter(f"{prefix}*"):
            self._client.delete(key)

    def clear(self) -> None:
        # Осознанно не делаем FLUSHDB — Redis может быть общим для других нужд.
        pass


_backend = _RedisCache(_redis_client) if _redis_client else _InMemoryCache()

# Ключи и TTL общие для bot.py (пишет/читает список игр) и app.py/CRM (обязан
# сбрасывать кэш при создании/редактировании игр и заявок). Держим их здесь,
# а не дублируем в каждом модуле — иначе легко забыть про инвалидацию при
# правке одного из модулей и получить рассинхронизацию, как это и произошло:
# CRM создавала игры через database.py и вообще не знала о существовании
# этого кэша, поэтому бот мог показывать устаревший список до истечения TTL
# или перезапуска процесса.
GAMES_CACHE_KEY = "games:upcoming_with_slots"
GAMES_CACHE_TTL = 30  # секунд — инвалидация при записи/правках всё равно
                      # сбрасывает ключ; TTL — запас, если процессы без Redis
LEVELS_CACHE_KEY = "levels:list"
LEVELS_CACHE_TTL = 3600
USER_CACHE_PREFIX = "user:tg:"
USER_CACHE_TTL = 120  # секунд — профиль почти не меняется между кликами меню


def get(key: str) -> Any:
    return _backend.get(key)


def set(key: str, value: Any, ttl: Optional[float] = None) -> None:
    _backend.set(key, value, ttl)


def delete(key: str) -> None:
    _backend.delete(key)


def delete_prefix(prefix: str) -> None:
    _backend.delete_prefix(prefix)


def invalidate_games_cache() -> None:
    """Сбрасывает кэш списка игр. Вызывать из ЛЮБОГО места, где меняются
    игры или количество занятых мест: создание/редактирование игры в CRM,
    запись/отмена в боте, смена статуса заявки в CRM (влияет на taken)."""
    delete(GAMES_CACHE_KEY)


def get_or_set(key: str, loader: Callable[[], Any], ttl: Optional[float] = None) -> Any:
    """Возвращает значение из кэша, а если его нет — вызывает loader(),
    кладёт результат в кэш и возвращает его. Основной паттерн cache-aside."""
    cached = get(key)
    if cached is not None:
        return cached
    value = loader()
    set(key, value, ttl)
    return value


def backend_name() -> str:
    return "redis" if _redis_client else "memory"
