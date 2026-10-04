# core/vwap_arbitrage.py
"""
Арбитраж на основе VWAP из локального стакана.

Для каждой пары и направления:
1. Берём ask-стакан на бирже A → VWAP для покупки $V.
2. Берём bid-стакан на бирже B → VWAP для продажи $V.
3. Считаем gross, fees, slippage, net.

ПРОВЕРКИ (порядок важен):
1. Стакан существует и НЕ пустой.
2. valid=True (стакан не помечен stale после gap).
3. local_age (ts) <= MAX_QUOTE_AGE_MS (250 мс).
4. skew_ms <= MAX_QUOTE_SKEW_MS (150 мс).
5. engine_age (cts) <= EXCHANGE_MAX_AGE_MS[биржа] — для Bybit.
6. exchange_age (ts) <= EXCHANGE_MAX_AGE_MS[биржа] — для остальных.
7. ЖЁСТКАЯ отсечка: local_age > 2000 мс → return None (страховка).

Жёсткая отсечка нужна, потому что Gate/MEXC могут "молчать"
по WS, но ts обновляется только при set_snapshot (REST reload).
Без неё бот может торговать по 30-секундным ценам.
"""
import time
from typing import List, Optional

from loguru import logger

from config import (
    FEES,
    MIN_SPREAD_PCT, MIN_PROFIT_USD,
    MAX_QUOTE_AGE_MS, MAX_QUOTE_SKEW_MS,
    MAX_EXECUTION_SLIPPAGE_PCT,
    TRADE_SIZES_USD,
)
from core.orderbook import orderbook_store


# Индивидуальные пороги возраста по биржам (мс).
# KuCoin/Gate могут "молчать" дольше — им даём запас.
EXCHANGE_MAX_AGE_MS = {
    "kucoin": 500,
    "bybit": 300,
    "mexc": 300,
    "gate": 500,
}

# Жёсткая отсечка: если локальный возраст стакана превышает это —
# не торгуем, даже если формально все проверки пройдены.
HARD_MAX_LOCAL_AGE_MS = 2000


def _default_fee(exchange: str) -> float:
    return FEES.get(exchange, 0.10)


def _exchange_max_age(exchange: str) -> int:
    return EXCHANGE_MAX_AGE_MS.get(exchange, MAX_QUOTE_AGE_MS)


def compute_vwap_arbitrage(
    symbol: str,
    buy_exchange: str,
    sell_exchange: str,
    volume_usd: float,
    now: Optional[float] = None,
) -> Optional[dict]:
    """
    Рассчитать VWAP-арбитраж для объёма volume_usd.

    Возвращает dict или None, если:
      - нет стакана (None)
      - стакан пустой
      - valid=False (после gap)
      - local_age > MAX_QUOTE_AGE_MS
      - skew_ms > MAX_QUOTE_SKEW_MS
      - exchange_age/engine_age > EXCHANGE_MAX_AGE_MS[биржа]
      - local_age > HARD_MAX_LOCAL_AGE_MS (жёсткая отсечка)
      - стакан не покрывает объём (>1% не набран)
      - execution slippage > MAX_EXECUTION_SLIPPAGE_PCT
    """
    if now is None:
        now = time.time()

    buy_book = orderbook_store.get(buy_exchange, symbol)
    sell_book = orderbook_store.get(sell_exchange, symbol)
    if buy_book is None or sell_book is None:
        return None

    # Стакан пустой (нечего считать)
    if buy_book.is_empty() or sell_book.is_empty():
        return None

    # <<< Проверка валидности (после gap)
    if not buy_book.valid or not sell_book.valid:
        return None

    buy_ts = buy_book.ts
    sell_ts = sell_book.ts
    if buy_ts <= 0 or sell_ts <= 0:
        return None

    buy_age_ms = max(0, int((now - buy_ts) * 1000))
    sell_age_ms = max(0, int((now - sell_ts) * 1000))
    skew_ms = abs(buy_age_ms - sell_age_ms)

    # <<< ЖЁСТКАЯ отсечка: страховка от застывших стаканов
    # Срабатывает раньше остальных проверок, чтобы не тратить CPU
    if buy_age_ms > HARD_MAX_LOCAL_AGE_MS:
        return None
    if sell_age_ms > HARD_MAX_LOCAL_AGE_MS:
        return None

    if buy_age_ms > MAX_QUOTE_AGE_MS:
        return None
    if sell_age_ms > MAX_QUOTE_AGE_MS:
        return None
    if skew_ms > MAX_QUOTE_SKEW_MS:
        return None

    # <<< Проверка реального возраста по engine_ts (cts) — Bybit
    buy_engine_age = buy_book.real_engine_age_ms(now=now)
    sell_engine_age = sell_book.real_engine_age_ms(now=now)

    if buy_engine_age >= 0:
        max_age = _exchange_max_age(buy_exchange)
        if buy_engine_age > max_age:
            return None
    if sell_engine_age >= 0:
        max_age = _exchange_max_age(sell_exchange)
        if sell_engine_age > max_age:
            return None

    # Fallback: если engine_ts нет, используем exchange_ts
    buy_exchange_age = buy_book.real_age_ms(now=now)
    sell_exchange_age = sell_book.real_age_ms(now=now)

    if buy_exchange_age >= 0 and buy_engine_age < 0:
        max_age = _exchange_max_age(buy_exchange)
        if buy_exchange_age > max_age:
            return None
    if sell_exchange_age >= 0 and sell_engine_age < 0:
        max_age = _exchange_max_age(sell_exchange)
        if sell_exchange_age > max_age:
            return None

    # ---- VWAP ----
    buy_res = buy_book.vwap_buy(volume_usd)
    sell_res = sell_book.vwap_sell(volume_usd)
    if buy_res is None or sell_res is None:
        return None

    buy_vwap, buy_filled, buy_best = buy_res
    sell_vwap, sell_filled, sell_best = sell_res

    # Стакан не покрывает объём
    if buy_filled < volume_usd * 0.99:
        return None
    if sell_filled < volume_usd * 0.99:
        return None

    # Execution slippage (VWAP vs best price)
    buy_slip_pct = (buy_vwap - buy_best) / buy_best * 100.0
    sell_slip_pct = (sell_best - sell_vwap) / sell_best * 100.0

    if buy_slip_pct > MAX_EXECUTION_SLIPPAGE_PCT:
        return None
    if sell_slip_pct > MAX_EXECUTION_SLIPPAGE_PCT:
        return None

    # ---- Экономика ----
    gross_pct = (sell_vwap - buy_vwap) / buy_vwap * 100.0
    fee_pct = _default_fee(buy_exchange) + _default_fee(sell_exchange)
    slip_pct = buy_slip_pct + sell_slip_pct
    net_pct = gross_pct - fee_pct - slip_pct
    net_profit_usd = volume_usd * net_pct / 100.0

    return {
        "symbol": symbol,
        "buy_exchange": buy_exchange,
        "sell_exchange": sell_exchange,
        "buy_price": buy_vwap,
        "sell_price": sell_vwap,
        "buy_best": buy_best,
        "sell_best": sell_best,
        "buy_slip_pct": buy_slip_pct,
        "sell_slip_pct": sell_slip_pct,
        "spread_pct": gross_pct,
        "fee_pct": fee_pct,
        "slippage_pct": slip_pct,
        "net_spread_pct": net_pct,
        "net_profit_usd": net_profit_usd,
        "volume_usd": volume_usd,
        "buy_age_ms": buy_age_ms,
        "sell_age_ms": sell_age_ms,
        "skew_ms": skew_ms,
        "is_stale": False,
        "sizes": None,
    }


def compute_multi_size(
    symbol: str,
    buy_exchange: str,
    sell_exchange: str,
    sizes: Optional[List[float]] = None,
    now: Optional[float] = None,
) -> Optional[dict]:
    """Рассчитать арбитраж для нескольких объёмов одновременно."""
    if sizes is None:
        sizes = TRADE_SIZES_USD

    if not sizes:
        return None

    results = []
    base = None
    for v in sizes:
        arb = compute_vwap_arbitrage(
            symbol, buy_exchange, sell_exchange, v, now=now
        )
        if arb is None:
            results.append({"volume": v, "net_pct": None, "net_profit_usd": None})
        else:
            results.append({
                "volume": v,
                "net_pct": arb["net_spread_pct"],
                "net_profit_usd": arb["net_profit_usd"],
            })
            if base is None:
                base = arb

    if base is None:
        return None

    base["sizes"] = results
    return base


def find_vwap_opportunities(
    exchanges,
    symbols,
    volume_usd: float,
    multi_size: bool = False,
):
    """Найти все VWAP-арбитражи."""
    opps = []
    now = time.time()

    for symbol in symbols:
        for buy_ex in exchanges:
            for sell_ex in exchanges:
                if buy_ex == sell_ex:
                    continue

                if multi_size:
                    arb = compute_multi_size(
                        symbol, buy_ex, sell_ex, now=now
                    )
                else:
                    arb = compute_vwap_arbitrage(
                        symbol, buy_ex, sell_ex, volume_usd, now=now
                    )

                if arb is None:
                    continue

                if arb["net_spread_pct"] < MIN_SPREAD_PCT:
                    continue
                if arb["net_profit_usd"] < MIN_PROFIT_USD:
                    continue

                opps.append(arb)

    opps.sort(key=lambda x: x["net_profit_usd"], reverse=True)
    return opps