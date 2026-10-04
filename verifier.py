# core/verifier.py
"""
REST-верификация стаканов перед отправкой сигналов.

Проблема: WS-стаканы KuCoin/Bybit могут быть устаревшими:
- KuCoin шлёт "пустые" обновления (ts меняется, цена нет)
- Bybit пропускает дельты (стакан "застревает")

Решение: для 🚀 (net > 3%) дёргаем REST и сравниваем.
"""
import aiohttp
from loguru import logger


KUCOIN_REST = "https://api.kucoin.com/api/v1/market/orderbook/level1"
BYBIT_REST = "https://api.bybit.com/v5/market/orderbook"
MEXC_REST = "https://api.mexc.com/api/v3/depth"
GATE_REST = "https://api.gateio.ws/api/v4/spot/order_book"


def _to_kucoin_symbol(symbol: str) -> str:
    return symbol.upper()  # BTC-USDT


def _to_bybit_symbol(symbol: str) -> str:
    return symbol.replace("-", "").upper()  # BTCUSDT


def _to_mexc_symbol(symbol: str) -> str:
    return symbol.replace("-", "").upper()


def _to_gate_symbol(symbol: str) -> str:
    return symbol.replace("-", "_")


async def fetch_rest_price(exchange: str, symbol: str) -> dict | None:
    """
    Вернуть {"bid": float, "ask": float} или None.
    """
    try:
        async with aiohttp.ClientSession() as session:
            if exchange == "kucoin":
                url = f"{KUCOIN_REST}?symbol={_to_kucoin_symbol(symbol)}"
                async with session.get(url, timeout=5) as r:
                    data = await r.json()
                    if data.get("code") != "200000":
                        return None
                    d = data["data"]
                    return {"bid": float(d["bestBid"]), "ask": float(d["bestAsk"])}

            elif exchange == "bybit":
                params = {
                    "category": "spot",
                    "symbol": _to_bybit_symbol(symbol),
                }
                async with session.get(BYBIT_REST, params=params, timeout=5) as r:
                    data = await r.json()
                    if data.get("retCode") != 0:
                        return None
                    d = data["result"]
                    return {
                        "bid": float(d["b"][0][0]),
                        "ask": float(d["a"][0][0]),
                    }

            elif exchange == "mexc":
                params = {"symbol": _to_mexc_symbol(symbol), "limit": 5}
                async with session.get(MEXC_REST, params=params, timeout=5) as r:
                    data = await r.json()
                    return {
                        "bid": float(data["bids"][0][0]),
                        "ask": float(data["asks"][0][0]),
                    }

            elif exchange == "gate":
                params = {
                    "currency_pair": _to_gate_symbol(symbol),
                    "limit": 5,
                }
                async with session.get(GATE_REST, params=params, timeout=5) as r:
                    data = await r.json()
                    return {
                        "bid": float(data["bids"][0][0]),
                        "ask": float(data["asks"][0][0]),
                    }
    except Exception as e:
        logger.warning(f"REST verify error {exchange} {symbol}: {e}")
        return None


async def verify_signal(op: dict, tolerance_pct: float = 0.3) -> bool:
    """
    Проверить, что WS-цена совпадает с REST.

    tolerance_pct: допустимое расхождение в % (по умолчанию 0.3%).

    Returns True если сигнал реальный, False если устарел.
    """
    symbol = op["symbol"]
    buy_ex = op["buy_exchange"]
    sell_ex = op["sell_exchange"]

    buy_ws = op["buy_price"]
    sell_ws = op["sell_price"]

    buy_rest = await fetch_rest_price(buy_ex, symbol)
    sell_rest = await fetch_rest_price(sell_ex, symbol)

    if buy_rest is None or sell_rest is None:
        logger.warning(f"REST verify: нет данных для {symbol}")
        return True  # не можем проверить — пропускаем

    # Сравниваем buy (ask)
    buy_diff_pct = abs(buy_ws - buy_rest["ask"]) / buy_rest["ask"] * 100
    # Сравниваем sell (bid)
    sell_diff_pct = abs(sell_ws - sell_rest["bid"]) / sell_rest["bid"] * 100

    if buy_diff_pct > tolerance_pct or sell_diff_pct > tolerance_pct:
        logger.warning(
            f"REST verify FAIL {symbol} {buy_ex}->{sell_ex}: "
            f"buy WS={buy_ws:.6g} REST={buy_rest['ask']:.6g} "
            f"(diff={buy_diff_pct:.2f}%), "
            f"sell WS={sell_ws:.6g} REST={sell_rest['bid']:.6g} "
            f"(diff={sell_diff_pct:.2f}%)"
        )
        return False

    logger.debug(
        f"REST verify OK {symbol} {buy_ex}->{sell_ex}: "
        f"buy diff={buy_diff_pct:.3f}%, sell diff={sell_diff_pct:.3f}%"
    )
    return True