# core/pair_discovery.py
"""
Автоматический отбор арбитражных пар Bybit ∩ MEXC ∩ Gate ∩ KuCoin.

Логика:
1. Тянем все USDT spot пары с Bybit, MEXC, Gate и KuCoin через REST.
2. Нормализуем символы к виду "BTC-USDT".
3. Берём пересечение четырёх бирж.
4. Тянем 24h ticker с четырёх бирж — получаем bid/ask/quoteVolume.
5. Фильтруем по объёму и внутреннему spread на каждой бирже.
6. Сортируем по min(volume_bybit, volume_mexc, volume_gate, volume_kucoin)
   и берём top MAX_ACTIVE_PAIRS.
"""
import asyncio
import re
from typing import Optional, List

import aiohttp
from loguru import logger

from config import (
    MIN_VOLUME_USDT, MAX_ACTIVE_PAIRS,
    MAX_INTERNAL_SPREAD_PCT, EXCLUDED_BASES,
    PRINT_DISCOVERED_PAIRS,
)


# --------------------------------------------------------------------- #
# REST endpoints                                                        #
# --------------------------------------------------------------------- #
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_TICKERS_URL     = "https://api.bybit.com/v5/market/tickers"

MEXC_EXCHANGE_INFO_URL = "https://api.mexc.com/api/v3/exchangeInfo"
MEXC_TICKER_24HR_URL   = "https://api.mexc.com/api/v3/ticker/24hr"

GATE_CURRENCY_PAIRS_URL = "https://api.gateio.ws/api/v4/spot/currency_pairs"
GATE_TICKERS_URL        = "https://api.gateio.ws/api/v4/spot/tickers"

KUCOIN_SYMBOLS_URL = "https://api.kucoin.com/api/v2/symbols"
KUCOIN_TICKERS_URL = "https://api.kucoin.com/api/v1/market/allTickers"


# База — только латинские буквы и цифры, 2–15 символов.
# Отсекает "龙虾", "测试", и другие экзотические тикеры.
_BASE_RE = re.compile(r"^[A-Z0-9]{2,15}$")


def normalize_pair(symbol: str) -> Optional[str]:
    """
    Приводит:
        BTCUSDT
        BTC_USDT
        BTC-USDT
    к единому виду: BTC-USDT.
    Возвращает None, если не USDT-пара или формат не распознан,
    либо если base содержит не-ASCII символы.
    """
    if not symbol:
        return None

    symbol = symbol.upper().replace("_", "-")

    if "-" not in symbol:
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            if not base or not _BASE_RE.match(base):
                return None
            return f"{base}-USDT"
        return None

    base, quote = symbol.split("-", 1)
    if quote != "USDT" or not base:
        return None
    if not _BASE_RE.match(base):
        return None
    return f"{base}-USDT"


# --------------------------------------------------------------------- #
# Bybit                                                                 #
# --------------------------------------------------------------------- #
async def get_bybit_pairs(session: aiohttp.ClientSession) -> dict:
    """
    Все активные USDT spot пары на Bybit.

    REST: GET https://api.bybit.com/v5/market/instruments-info?category=spot
    Ответ: {"retCode":0,"result":{"list":[
              {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT",
               "status":"Trading",...}, ...]}}
    """
    params = {"category": "spot"}
    try:
        async with session.get(BYBIT_INSTRUMENTS_URL, params=params, timeout=15) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"Bybit pair discovery error: {e}")
        return {}

    if data.get("retCode") != 0:
        logger.error(f"Bybit instruments retCode={data.get('retCode')}")
        return {}

    result = {}
    for item in data.get("result", {}).get("list", []):
        symbol = item.get("symbol")
        quote = item.get("quoteCoin")
        base = item.get("baseCoin")
        status = item.get("status")

        if quote != "USDT":
            continue
        if status != "Trading":
            continue
        if base in EXCLUDED_BASES:
            continue

        pair = normalize_pair(symbol)
        if pair:
            result[pair] = {"symbol": symbol, "base": base, "quote": quote}

    logger.info(f"Bybit: найдено {len(result)} активных USDT spot пар")
    return result


async def get_bybit_tickers(session: aiohttp.ClientSession) -> dict:
    """
    24h тикеры Bybit: bid/ask/quoteVolume.

    REST: GET https://api.bybit.com/v5/market/tickers?category=spot

    ВАЖНО: Bybit не отдаёт поле "quoteVolume".
    Вместо него:
      - turnover24h — оборот в USDT (то, что нам нужно)
      - volume24h   — оборот в базовом активе (BTC/ETH/...)

    Ответ: {"retCode":0,"result":{"list":[
              {"symbol":"BTCUSDT","bid1Price":"...","ask1Price":"...",
               "turnover24h":"494046735.42","volume24h":"5781.12",...}, ...]}}
    """
    params = {"category": "spot"}
    try:
        async with session.get(BYBIT_TICKERS_URL, params=params, timeout=20) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"Bybit ticker error: {e}")
        return {}

    if data.get("retCode") != 0:
        logger.error(f"Bybit tickers retCode={data.get('retCode')}")
        return {}

    result = {}
    for item in data.get("result", {}).get("list", []):
        symbol = item.get("symbol")
        pair = normalize_pair(symbol or "")
        if not pair:
            continue
        try:
            bid = float(item.get("bid1Price") or 0)
            ask = float(item.get("ask1Price") or 0)
            # Bybit: turnover24h — оборот в USDT.
            # Fallback на quoteVolume — если Bybit когда-нибудь поменяет схему.
            quote_volume = float(
                item.get("turnover24h")
                or item.get("quoteVolume")
                or 0
            )
        except (TypeError, ValueError):
            continue
        if bid <= 0 or ask <= 0 or quote_volume <= 0:
            continue

        spread_pct = ((ask - bid) / ((ask + bid) / 2)) * 100
        result[pair] = {
            "bid": bid, "ask": ask,
            "quote_volume": quote_volume,
            "spread_pct": spread_pct,
        }
    return result


# --------------------------------------------------------------------- #
# MEXC                                                                  #
# --------------------------------------------------------------------- #
async def get_mexc_pairs(session: aiohttp.ClientSession) -> dict:
    """Все активные USDT spot пары на MEXC."""
    try:
        async with session.get(MEXC_EXCHANGE_INFO_URL, timeout=15) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"MEXC pair discovery error: {e}")
        return {}

    result = {}
    for item in data.get("symbols", []):
        symbol = item.get("symbol")
        status = item.get("status")
        quote = item.get("quoteAsset")
        base = item.get("baseAsset")

        if not symbol or quote != "USDT":
            continue
        if status not in ("1", "ENABLED"):
            continue
        if item.get("isSpotTradingAllowed") is False:
            continue
        if base in EXCLUDED_BASES:
            continue

        pair = normalize_pair(symbol)
        if pair:
            result[pair] = {"symbol": symbol, "base": base, "quote": quote}

    logger.info(f"MEXC: найдено {len(result)} активных USDT spot пар")
    return result


async def get_mexc_tickers(session: aiohttp.ClientSession) -> dict:
    """24h тикеры MEXC: bid/ask/quoteVolume."""
    try:
        async with session.get(MEXC_TICKER_24HR_URL, timeout=20) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"MEXC ticker error: {e}")
        return {}

    result = {}
    for item in data:
        pair = normalize_pair(item.get("symbol") or "")
        if not pair:
            continue
        try:
            bid = float(item.get("bidPrice") or 0)
            ask = float(item.get("askPrice") or 0)
            quote_volume = float(item.get("quoteVolume") or 0)
        except (TypeError, ValueError):
            continue
        if bid <= 0 or ask <= 0 or quote_volume <= 0:
            continue

        spread_pct = ((ask - bid) / ((ask + bid) / 2)) * 100
        result[pair] = {
            "bid": bid, "ask": ask,
            "quote_volume": quote_volume,
            "spread_pct": spread_pct,
        }
    return result


# --------------------------------------------------------------------- #
# Gate                                                                  #
# --------------------------------------------------------------------- #
async def get_gate_pairs(session: aiohttp.ClientSession) -> dict:
    """Все активные USDT spot пары на Gate."""
    try:
        async with session.get(GATE_CURRENCY_PAIRS_URL, timeout=15) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"Gate pair discovery error: {e}")
        return {}

    result = {}
    for item in data:
        symbol = item.get("id")
        base = item.get("base")
        quote = item.get("quote")
        status = item.get("trade_status")

        if quote != "USDT":
            continue
        if status != "tradable":
            continue
        if base in EXCLUDED_BASES:
            continue

        pair = normalize_pair(symbol)
        if pair:
            result[pair] = {"symbol": symbol, "base": base, "quote": quote}

    logger.info(f"Gate: найдено {len(result)} активных USDT spot пар")
    return result


async def get_gate_tickers(session: aiohttp.ClientSession) -> dict:
    """24h тикеры Gate: bid/ask/quoteVolume."""
    try:
        async with session.get(GATE_TICKERS_URL, timeout=20) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"Gate ticker error: {e}")
        return {}

    result = {}
    for item in data:
        pair = normalize_pair(item.get("currency_pair") or "")
        if not pair:
            continue
        try:
            bid = float(item.get("highest_bid") or 0)
            ask = float(item.get("lowest_ask") or 0)
            quote_volume = float(item.get("quote_volume") or 0)
        except (TypeError, ValueError):
            continue
        if bid <= 0 or ask <= 0 or quote_volume <= 0:
            continue

        spread_pct = ((ask - bid) / ((ask + bid) / 2)) * 100
        result[pair] = {
            "bid": bid, "ask": ask,
            "quote_volume": quote_volume,
            "spread_pct": spread_pct,
        }
    return result


# --------------------------------------------------------------------- #
# KuCoin                                                                #
# --------------------------------------------------------------------- #
async def get_kucoin_pairs(session: aiohttp.ClientSession) -> dict:
    """Все активные USDT spot пары на KuCoin."""
    try:
        async with session.get(KUCOIN_SYMBOLS_URL, timeout=15) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"KuCoin pair discovery error: {e}")
        return {}

    if str(data.get("code")) != "200000":
        logger.error(f"KuCoin symbols code={data.get('code')}")
        return {}

    result = {}
    for item in data.get("data", []):
        symbol = item.get("symbol")
        base = item.get("baseCurrency")
        quote = item.get("quoteCurrency")
        enable_trading = item.get("enableTrading")

        if quote != "USDT":
            continue
        if enable_trading is False:
            continue
        if base in EXCLUDED_BASES:
            continue

        pair = normalize_pair(symbol)
        if pair:
            result[pair] = {"symbol": symbol, "base": base, "quote": quote}

    logger.info(f"KuCoin: найдено {len(result)} активных USDT spot пар")
    return result


async def get_kucoin_tickers(session: aiohttp.ClientSession) -> dict:
    """24h тикеры KuCoin: bid/ask/quoteVolume."""
    try:
        async with session.get(KUCOIN_TICKERS_URL, timeout=20) as r:
            data = await r.json()
    except Exception as e:
        logger.error(f"KuCoin ticker error: {e}")
        return {}

    if str(data.get("code")) != "200000":
        logger.error(f"KuCoin tickers code={data.get('code')}")
        return {}

    result = {}
    tickers = data.get("data", {}).get("ticker", [])
    for item in tickers:
        symbol = item.get("symbol")
        pair = normalize_pair(symbol or "")
        if not pair:
            continue
        try:
            bid = float(item.get("buy") or 0)
            ask = float(item.get("sell") or 0)
            quote_volume = float(item.get("volValue") or 0)
        except (TypeError, ValueError):
            continue
        if bid <= 0 or ask <= 0 or quote_volume <= 0:
            continue

        spread_pct = ((ask - bid) / ((ask + bid) / 2)) * 100
        result[pair] = {
            "bid": bid, "ask": ask,
            "quote_volume": quote_volume,
            "spread_pct": spread_pct,
        }
    return result


# --------------------------------------------------------------------- #
# Discovery                                                             #
# --------------------------------------------------------------------- #
async def discover_arbitrage_pairs() -> List[str]:
    """
    Возвращает отфильтрованный список пар, готовых к WS-подписке.
    Пересечение четырёх бирж: Bybit ∩ MEXC ∩ Gate ∩ KuCoin.
    """
    async with aiohttp.ClientSession() as session:
        bybit_pairs, mexc_pairs, gate_pairs, kucoin_pairs = await asyncio.gather(
            get_bybit_pairs(session),
            get_mexc_pairs(session),
            get_gate_pairs(session),
            get_kucoin_pairs(session),
        )

        if not bybit_pairs or not mexc_pairs or not gate_pairs or not kucoin_pairs:
            logger.warning(
                f"Не удалось получить пары: "
                f"Bybit={len(bybit_pairs)}, MEXC={len(mexc_pairs)}, "
                f"Gate={len(gate_pairs)}, KuCoin={len(kucoin_pairs)}"
            )
            return []

        common = (
            set(bybit_pairs)
            & set(mexc_pairs)
            & set(gate_pairs)
            & set(kucoin_pairs)
        )
        logger.info(f"Пересечение Bybit/MEXC/Gate/KuCoin: {len(common)} пар")

        (
            bybit_tickers,
            mexc_tickers,
            gate_tickers,
            kucoin_tickers,
        ) = await asyncio.gather(
            get_bybit_tickers(session),
            get_mexc_tickers(session),
            get_gate_tickers(session),
            get_kucoin_tickers(session),
        )

    # Диагностика: если биржа не отдала ни одного тикера — предупреждаем
    logger.debug(
        f"tickers: Bybit={len(bybit_tickers)}, MEXC={len(mexc_tickers)}, "
        f"Gate={len(gate_tickers)}, KuCoin={len(kucoin_tickers)}"
    )

    candidates = []
    # Диагностика причин отсева
    reject_reasons = {
        "no_bybit": 0, "no_mexc": 0, "no_gate": 0, "no_kucoin": 0,
        "low_bybit_vol": 0, "low_mexc_vol": 0,
        "low_gate_vol": 0, "low_kucoin_vol": 0,
        "high_bybit_spread": 0, "high_mexc_spread": 0,
        "high_gate_spread": 0, "high_kucoin_spread": 0,
    }

    for pair in common:
        b = bybit_tickers.get(pair)
        m = mexc_tickers.get(pair)
        g = gate_tickers.get(pair)
        k = kucoin_tickers.get(pair)
        if not b:
            reject_reasons["no_bybit"] += 1
            continue
        if not m:
            reject_reasons["no_mexc"] += 1
            continue
        if not g:
            reject_reasons["no_gate"] += 1
            continue
        if not k:
            reject_reasons["no_kucoin"] += 1
            continue

        # Объём должен быть достаточным на ВСЕХ четырёх биржах
        if b["quote_volume"] < MIN_VOLUME_USDT:
            reject_reasons["low_bybit_vol"] += 1
            continue
        if m["quote_volume"] < MIN_VOLUME_USDT:
            reject_reasons["low_mexc_vol"] += 1
            continue
        if g["quote_volume"] < MIN_VOLUME_USDT:
            reject_reasons["low_gate_vol"] += 1
            continue
        if k["quote_volume"] < MIN_VOLUME_USDT:
            reject_reasons["low_kucoin_vol"] += 1
            continue

        # Внутренний spread на каждой бирже
        if b["spread_pct"] > MAX_INTERNAL_SPREAD_PCT:
            reject_reasons["high_bybit_spread"] += 1
            continue
        if m["spread_pct"] > MAX_INTERNAL_SPREAD_PCT:
            reject_reasons["high_mexc_spread"] += 1
            continue
        if g["spread_pct"] > MAX_INTERNAL_SPREAD_PCT:
            reject_reasons["high_gate_spread"] += 1
            continue
        if k["spread_pct"] > MAX_INTERNAL_SPREAD_PCT:
            reject_reasons["high_kucoin_spread"] += 1
            continue

        # Для рейтинга берём минимальный объём среди четырёх бирж.
        # Это гарантирует, что обе стороны сделки исполнятся.
        liquidity = min(
            b["quote_volume"],
            m["quote_volume"],
            g["quote_volume"],
            k["quote_volume"],
        )
        candidates.append({
            "pair": pair,
            "bybit_volume": b["quote_volume"],
            "mexc_volume": m["quote_volume"],
            "gate_volume": g["quote_volume"],
            "kucoin_volume": k["quote_volume"],
            "liquidity": liquidity,
            "bybit_spread": b["spread_pct"],
            "mexc_spread": m["spread_pct"],
            "gate_spread": g["spread_pct"],
            "kucoin_spread": k["spread_pct"],
        })

    # Диагностика: если 0 кандидатов — показываем причины
    if not candidates and common:
        nonzero_reasons = {k: v for k, v in reject_reasons.items() if v > 0}
        if nonzero_reasons:
            logger.warning(f"Причины отсева пар: {nonzero_reasons}")

    # Сначала самые ликвидные (по слабейшей бирже)
    candidates.sort(key=lambda x: x["liquidity"], reverse=True)
    selected = candidates[:MAX_ACTIVE_PAIRS]

    logger.info(f"После фильтра ликвидности: {len(candidates)} пар")
    logger.info(f"Активный список: {len(selected)} пар")

    if PRINT_DISCOVERED_PAIRS:
        logger.info("===== ARBITRAGE PAIRS =====")
        for i, item in enumerate(selected, 1):
            logger.info(
                f"{i:02d}. {item['pair']} | "
                f"Bybit=${item['bybit_volume']:,.0f} | "
                f"MEXC=${item['mexc_volume']:,.0f} | "
                f"Gate=${item['gate_volume']:,.0f} | "
                f"KuCoin=${item['kucoin_volume']:,.0f} | "
                f"min=${item['liquidity']:,.0f}"
            )
        logger.info("===========================")

    return [item["pair"] for item in selected]