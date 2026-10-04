# core/active_symbols.py
"""
Глобальный потокобезопасный список активных символов.

Используется:
- main.py — обновляет список через pair_discovery
- gate_ws.py / mexc_ws.py — читают список для подписки
- spread.py — фильтрует возможности по активным символам
"""
from threading import Lock
from typing import List


_lock = Lock()
_symbols: List[str] = []


def update_symbols(new_symbols: List[str]) -> None:
    """Атомарно заменить список активных символов."""
    global _symbols
    with _lock:
        _symbols = list(new_symbols)


def get_symbols() -> List[str]:
    """Копия текущего списка активных символов."""
    with _lock:
        return list(_symbols)


def is_active(symbol: str) -> bool:
    """Проверить, активен ли символ сейчас."""
    with _lock:
        return symbol in _symbols


def count() -> int:
    with _lock:
        return len(_symbols)