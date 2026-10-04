# core/orderbook.py
"""
Локальный стакан для каждой (exchange, symbol).

Хранит N уровней bid/ask, поддерживает VWAP-расчёт для заданного объёма.
Потокобезопасен: чтение из arbitrage_loop, запись из WS-потоков.

ВАЖНО: max_levels (ORDERBOOK_DEPTH) передаётся в __init__.
Стакан НЕ растёт бесконечно — обрезается после каждой модификации.

НОВОЕ:
- exchange_ts   — системное время биржи (ts из WS)
- engine_ts     — время matching engine (cts из WS)
- seq           — cross sequence (seq из WS)
- valid         — флаг валидности стакана (False после gap)
Это позволяет:
1. Считать реальный возраст данных (не локальный).
2. Не торговать на частичном стакане после gap.
"""
import time
from threading import RLock
from typing import Dict, List, Optional, Tuple


Level = Tuple[float, float]


class OrderBook:
    """Стакан одной пары на одной бирже."""

    __slots__ = (
        "bids", "asks", "ts",
        "exchange_ts", "engine_ts", "seq",
        "last_update_id", "max_levels", "valid", "_lock",
    )

    def __init__(self, max_levels: int = 20):
        self.bids: List[Level] = []   # desc by price
        self.asks: List[Level] = []   # asc by price
        self.ts: float = 0.0           # локальное время (когда Python получил)
        self.exchange_ts: float = 0.0  # системное время биржи (секунды)
        self.engine_ts: float = 0.0    # время matching engine (секунды)
        self.seq: int = 0              # cross sequence
        self.last_update_id: int = 0   # update ID (u)
        self.max_levels: int = max(1, int(max_levels))
        self.valid: bool = False       # валиден ли стакан
        self._lock = RLock()

    # ------------------------------------------------------------------ #
    # Обновление                                                         #
    # ------------------------------------------------------------------ #
    def set_snapshot(
        self,
        bids: List[Level],
        asks: List[Level],
        update_id: int = 0,
        exchange_ts: float = 0.0,
        engine_ts: float = 0.0,
        seq: int = 0,
    ) -> None:
        """Полный снимок стакана. Сбрасывает valid=True."""
        with self._lock:
            self.bids = sorted(
                ((float(p), float(q)) for p, q in bids if q > 0 and p > 0),
                key=lambda x: -x[0],
            )[: self.max_levels]
            self.asks = sorted(
                ((float(p), float(q)) for p, q in asks if q > 0 and p > 0),
                key=lambda x: x[0],
            )[: self.max_levels]
            self.ts = time.time()
            if exchange_ts > 0:
                self.exchange_ts = float(exchange_ts)
            if engine_ts > 0:
                self.engine_ts = float(engine_ts)
            if seq > 0:
                self.seq = int(seq)
            self.last_update_id = int(update_id)
            self.valid = True

    def update_meta(
        self,
        exchange_ts: float = 0.0,
        engine_ts: float = 0.0,
        seq: int = 0,
        update_id: int = 0,
    ) -> None:
        """Обновить только метаданные (для delta)."""
        with self._lock:
            if exchange_ts > 0:
                self.exchange_ts = float(exchange_ts)
            if engine_ts > 0:
                self.engine_ts = float(engine_ts)
            if seq > 0:
                self.seq = int(seq)
            if update_id > 0:
                self.last_update_id = int(update_id)
            self.ts = time.time()

    def mark_stale(self) -> None:
        """Пометить стакан невалидным (после gap)."""
        with self._lock:
            self.valid = False

    def apply_delta(self, side: str, price: float, qty: float) -> None:
        """side: 'bid' | 'ask'. qty=0 → удалить уровень."""
        price = float(price)
        qty = float(qty)
        if price <= 0:
            return

        book = self.bids if side == "bid" else self.asks

        with self._lock:
            idx = -1
            for i, (p, _) in enumerate(book):
                if p == price:
                    idx = i
                    break

            if idx >= 0:
                if qty <= 0:
                    book.pop(idx)
                else:
                    book[idx] = (price, qty)
            elif qty > 0:
                book.append((price, qty))
                if side == "bid":
                    book.sort(key=lambda x: -x[0])
                else:
                    book.sort(key=lambda x: x[0])

                if len(book) > self.max_levels:
                    del book[self.max_levels:]

            self.ts = time.time()

    def apply_deltas(self, side: str, levels: List[Level]) -> None:
        """Применить список уровней одной стороны."""
        if not levels:
            return

        book = self.bids if side == "bid" else self.asks

        with self._lock:
            price_to_idx = {p: i for i, (p, _) in enumerate(book)}

            for p_raw, q_raw in levels:
                try:
                    p = float(p_raw)
                    q = float(q_raw)
                except (TypeError, ValueError):
                    continue
                if p <= 0:
                    continue

                idx = price_to_idx.get(p)
                if idx is not None:
                    if q <= 0:
                        book[idx] = (p, 0.0)
                    else:
                        book[idx] = (p, q)
                elif q > 0:
                    book.append((p, q))
                    price_to_idx[p] = len(book) - 1

            book[:] = [lvl for lvl in book if lvl[1] > 0]

            if side == "bid":
                book.sort(key=lambda x: -x[0])
            else:
                book.sort(key=lambda x: x[0])

            if len(book) > self.max_levels:
                del book[self.max_levels:]

            self.ts = time.time()

    # ------------------------------------------------------------------ #
    # VWAP                                                               #
    # ------------------------------------------------------------------ #
    def vwap_buy(self, volume_usd: float) -> Optional[Tuple[float, float, float]]:
        """VWAP покупки (ask). Возвращает (vwap, filled_usd, best_price)."""
        with self._lock:
            if not self.asks or volume_usd <= 0:
                return None

            best_price = self.asks[0][0]
            remaining = volume_usd
            spent = 0.0
            qty_total = 0.0

            for price, qty in self.asks:
                level_usd = price * qty
                if level_usd >= remaining:
                    qty_take = remaining / price
                    spent += remaining
                    qty_total += qty_take
                    remaining = 0.0
                    break
                else:
                    spent += level_usd
                    qty_total += qty
                    remaining -= level_usd

            if qty_total <= 0:
                return None

            vwap = spent / qty_total
            return vwap, spent, best_price

    def vwap_sell(self, volume_usd: float) -> Optional[Tuple[float, float, float]]:
        """VWAP продажи (bid). Возвращает (vwap, filled_usd, best_price)."""
        with self._lock:
            if not self.bids or volume_usd <= 0:
                return None

            best_price = self.bids[0][0]
            remaining = volume_usd
            received = 0.0
            qty_total = 0.0

            for price, qty in self.bids:
                level_usd = price * qty
                if level_usd >= remaining:
                    qty_take = remaining / price
                    received += remaining
                    qty_total += qty_take
                    remaining = 0.0
                    break
                else:
                    received += level_usd
                    qty_total += qty
                    remaining -= level_usd

            if qty_total <= 0:
                return None

            vwap = received / qty_total
            return vwap, received, best_price

    # ------------------------------------------------------------------ #
    # Диагностика                                                        #
    # ------------------------------------------------------------------ #
    def best_bid(self) -> Optional[float]:
        with self._lock:
            return self.bids[0][0] if self.bids else None

    def best_ask(self) -> Optional[float]:
        with self._lock:
            return self.asks[0][0] if self.asks else None

    def is_empty(self) -> bool:
        with self._lock:
            return not self.bids and not self.asks

    def depth_bid(self) -> int:
        with self._lock:
            return len(self.bids)

    def depth_ask(self) -> int:
        with self._lock:
            return len(self.asks)

    def real_age_ms(self, now: Optional[float] = None) -> int:
        """Возраст по exchange_ts. -1 если неизвестен."""
        with self._lock:
            if self.exchange_ts <= 0:
                return -1
            if now is None:
                now = time.time()
            return max(0, int((now - self.exchange_ts) * 1000))

    def real_engine_age_ms(self, now: Optional[float] = None) -> int:
        """Возраст по engine_ts (cts). -1 если неизвестен."""
        with self._lock:
            if self.engine_ts <= 0:
                return -1
            if now is None:
                now = time.time()
            return max(0, int((now - self.engine_ts) * 1000))


class OrderBookStore:
    """Хранилище стаканов: {(exchange, symbol): OrderBook}."""

    def __init__(self, max_levels: int = 20):
        self._books: Dict[Tuple[str, str], OrderBook] = {}
        self._max_levels = max(1, int(max_levels))
        self._lock = RLock()

    def get_or_create(self, exchange: str, symbol: str) -> OrderBook:
        key = (exchange, symbol)
        with self._lock:
            book = self._books.get(key)
            if book is None:
                book = OrderBook(max_levels=self._max_levels)
                self._books[key] = book
            return book

    def get(self, exchange: str, symbol: str) -> Optional[OrderBook]:
        with self._lock:
            return self._books.get((exchange, symbol))

    def snapshot(self) -> Dict[Tuple[str, str], OrderBook]:
        with self._lock:
            return dict(self._books)

    def purge(self, exchange: str, symbol: str) -> None:
        with self._lock:
            self._books.pop((exchange, symbol), None)


from config import ORDERBOOK_DEPTH  # noqa: E402

orderbook_store = OrderBookStore(max_levels=ORDERBOOK_DEPTH)