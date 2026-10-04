# exchanges/bybit_ws.py
"""
Bybit Spot WebSocket — orderbook.50 (snapshot + delta).

Endpoint: wss://stream.bybit.com/v5/public/spot
Channel : orderbook.50.<SYMBOL>  (20 ms)

Формат:
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "snapshot" | "delta",
  "ts": 1672304484978,       # системное время Bybit
  "cts": 1672304484970,      # время matching engine (может отсутствовать в spot)
  "data": {
    "s": "BTCUSDT",
    "b": [["price", "size"], ...],
    "a": [["price", "size"], ...],
    "u": 18521288,
    "seq": 7961638724
  }
}

ПРАВИЛЬНЫЙ gap recovery:
1. При пропуске u != last_u + 1 → mark_stale() + REST resync.
2. REST snapshot → set_snapshot → valid=True, last_u обновляется.
3. Пока valid=False — сигналы не генерируются.

ФИКСЫ:
- cts fallback: если Bybit spot не отдаёт "cts" — используем "ts"
  как engine_ts, чтобы real_engine_age_ms возвращал осмысленное значение.
- Тот же fallback в REST-снапшоте (_fetch_rest_snapshot).
"""
import asyncio
import json
import threading
import time

import aiohttp
import websockets
from loguru import logger

from config import (
    WS_URLS, ORDERBOOK_DEPTH,
    BYBIT_ORDERBOOK_DEPTH, BYBIT_PING_INTERVAL, BYBIT_SUBSCRIBE_CHUNK,
)
from core.orderbook import orderbook_store
from core import active_symbols


BYBIT_REST_ORDERBOOK = "https://api.bybit.com/v5/market/orderbook"


def _to_bybit_symbol(symbol: str) -> str:
    return symbol.replace("-", "").upper()


def _from_bybit_symbol(symbol: str) -> str:
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if symbol.endswith(quote):
            return f"{symbol[:-len(quote)]}-{quote}"
    return symbol


async def _fetch_rest_snapshot(symbol: str):
    """Запросить REST snapshot для resync."""
    params = {
        "category": "spot",
        "symbol": _to_bybit_symbol(symbol),
        "limit": ORDERBOOK_DEPTH,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                BYBIT_REST_ORDERBOOK, params=params, timeout=5
            ) as r:
                data = await r.json()
                if data.get("retCode") != 0:
                    logger.warning(
                        f"Bybit REST snapshot retCode={data.get('retCode')}"
                    )
                    return None
                result = data["result"]

                # <<< ФИКС: cts может отсутствовать в spot → fallback на ts
                ts_val = result.get("ts", 0)
                cts_val = result.get("cts", 0)
                ts_exchange = ts_val / 1000.0 if ts_val else 0.0
                ts_engine = (cts_val / 1000.0) if cts_val else ts_exchange

                return {
                    "bids": [(float(p), float(q)) for p, q in result["b"]],
                    "asks": [(float(p), float(q)) for p, q in result["a"]],
                    "u": result.get("u", 0),
                    "seq": result.get("seq", 0),
                    "ts": ts_exchange,
                    "cts": ts_engine,
                }
    except Exception as e:
        logger.warning(f"Bybit REST snapshot error {symbol}: {e}")
        return None


class BybitWSClient:
    def __init__(self):
        self._stop = threading.Event()
        self._reconnect_delay = 2.0
        self._ws = None
        self._loop = None
        self._lock = threading.Lock()
        self._subscribed = set()
        # {symbol: last_u}
        self._last_u: dict[str, int] = {}
        # {symbol: True} — чтобы не запускать несколько resync
        self._resyncing: set[str] = set()

    async def run(self):
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Bybit WS error: {e}")

            if self._stop.is_set():
                break

            delay = self._reconnect_delay
            self._reconnect_delay = min(self._reconnect_delay * 1.5, 30.0)
            logger.warning(f"Bybit reconnect через {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        logger.info(f"Bybit connecting to {WS_URLS['bybit']}")
        async with websockets.connect(
            WS_URLS["bybit"],
            ping_interval=BYBIT_PING_INTERVAL,
            ping_timeout=10,
            close_timeout=5,
            max_size=16 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            self._loop = asyncio.get_running_loop()
            logger.success("Bybit WS connected")
            self._reconnect_delay = 2.0

            self._last_u.clear()
            self._resyncing.clear()

            symbols = active_symbols.get_symbols()
            if not symbols:
                logger.warning("Bybit: active_symbols пуст")
            else:
                await self._subscribe_all(ws, symbols)

            async for message in ws:
                try:
                    self._handle_message(message)
                except Exception as e:
                    logger.exception(f"Bybit message error: {e}")

    async def _subscribe_all(self, ws, symbols):
        topics = [
            f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{_to_bybit_symbol(s)}"
            for s in symbols
        ]
        chunk = BYBIT_SUBSCRIBE_CHUNK
        for i in range(0, len(topics), chunk):
            part = topics[i:i + chunk]
            msg = {"op": "subscribe", "args": part}
            try:
                await ws.send(json.dumps(msg))
            except Exception as e:
                logger.error(f"Bybit subscribe send error: {e}")
                return
            await asyncio.sleep(0.05)

        with self._lock:
            self._subscribed = set(symbols)
        logger.info(
            f"Bybit подписан на {len(symbols)} пар "
            f"(orderbook.{BYBIT_ORDERBOOK_DEPTH})"
        )

    def _handle_message(self, message):
        if not isinstance(message, str):
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        op = data.get("op")
        if op in ("subscribe", "unsubscribe"):
            if not data.get("success", True):
                logger.warning(f"Bybit {op} error: {data}")
            return

        if op == "pong":
            return

        topic = data.get("topic", "")
        prefix = f"orderbook.{BYBIT_ORDERBOOK_DEPTH}."
        if not topic.startswith(prefix):
            return

        msg_type = data.get("type")
        payload = data.get("data") or {}
        symbol_raw = payload.get("s")
        bids = payload.get("b") or []
        asks = payload.get("a") or []
        u = payload.get("u")
        seq = payload.get("seq", 0)

        # Времена: ms → секунды
        ts_exchange = data.get("ts", 0) / 1000.0 if data.get("ts") else 0.0
        # <<< ФИКС: Bybit spot может не отдавать cts.
        # Если cts отсутствует — используем ts как engine_ts.
        raw_cts = data.get("cts")
        if raw_cts:
            ts_engine = raw_cts / 1000.0
        else:
            ts_engine = ts_exchange

        if not symbol_raw:
            return

        symbol = _from_bybit_symbol(symbol_raw)
        book = orderbook_store.get_or_create("bybit", symbol)

        # ---- GAP DETECTION ----
        if msg_type == "delta" and u is not None:
            last_u = self._last_u.get(symbol)
            if last_u is not None and u != last_u + 1:
                logger.warning(
                    f"Bybit {symbol}: GAP! expected u={last_u + 1}, got u={u}. "
                    f"Marking stale + REST resync..."
                )
                book.mark_stale()
                self._last_u.pop(symbol, None)
                if symbol not in self._resyncing:
                    self._resyncing.add(symbol)
                    asyncio.create_task(self._resync_symbol(symbol))
                return
            self._last_u[symbol] = u

        # ---- SNAPSHOT ----
        if msg_type == "snapshot":
            try:
                book.set_snapshot(
                    [(float(p), float(q)) for p, q in bids],
                    [(float(p), float(q)) for p, q in asks],
                    update_id=u or 0,
                    exchange_ts=ts_exchange,
                    engine_ts=ts_engine,
                    seq=seq,
                )
                if u is not None:
                    self._last_u[symbol] = u
            except (TypeError, ValueError):
                return

        # ---- DELTA ----
        elif msg_type == "delta":
            try:
                if bids:
                    book.apply_deltas(
                        "bid", [(float(p), float(q)) for p, q in bids]
                    )
                if asks:
                    book.apply_deltas(
                        "ask", [(float(p), float(q)) for p, q in asks]
                    )
                book.update_meta(
                    exchange_ts=ts_exchange,
                    engine_ts=ts_engine,
                    seq=seq,
                )
            except (TypeError, ValueError):
                return

    async def _resync_symbol(self, symbol: str):
        """Запросить REST snapshot и заменить локальный стакан."""
        try:
            snapshot = await _fetch_rest_snapshot(symbol)
            if snapshot is None:
                logger.warning(f"Bybit resync failed for {symbol}")
                return
            book = orderbook_store.get_or_create("bybit", symbol)
            book.set_snapshot(
                snapshot["bids"],
                snapshot["asks"],
                update_id=snapshot["u"],
                exchange_ts=snapshot["ts"],
                engine_ts=snapshot["cts"],
                seq=snapshot["seq"],
            )
            self._last_u[symbol] = snapshot["u"]
            logger.info(
                f"Bybit {symbol} resynced: u={snapshot['u']}, "
                f"depth={len(snapshot['bids'])}/{len(snapshot['asks'])}"
            )
        finally:
            self._resyncing.discard(symbol)

    def request_resubscribe(self):
        if self._loop is None or self._ws is None:
            logger.warning("Bybit resubscribe: WS не подключён")
            return
        asyncio.run_coroutine_threadsafe(self._resubscribe(), self._loop)

    async def _resubscribe(self):
        ws = self._ws
        if ws is None or ws.closed:
            return

        new_symbols = set(active_symbols.get_symbols())
        with self._lock:
            old_symbols = set(self._subscribed)

        to_add = new_symbols - old_symbols
        to_remove = old_symbols - new_symbols

        if to_remove:
            topics = [
                f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{_to_bybit_symbol(s)}"
                for s in to_remove
            ]
            chunk = BYBIT_SUBSCRIBE_CHUNK
            for i in range(0, len(topics), chunk):
                part = topics[i:i + chunk]
                try:
                    await ws.send(json.dumps({"op": "unsubscribe", "args": part}))
                except Exception as e:
                    logger.error(f"Bybit unsubscribe send error: {e}")
                await asyncio.sleep(0.05)
            for sym in to_remove:
                orderbook_store.purge("bybit", sym)
                self._last_u.pop(sym, None)

        if to_add:
            topics = [
                f"orderbook.{BYBIT_ORDERBOOK_DEPTH}.{_to_bybit_symbol(s)}"
                for s in to_add
            ]
            chunk = BYBIT_SUBSCRIBE_CHUNK
            for i in range(0, len(topics), chunk):
                part = topics[i:i + chunk]
                try:
                    await ws.send(json.dumps({"op": "subscribe", "args": part}))
                except Exception as e:
                    logger.error(f"Bybit subscribe send error: {e}")
                await asyncio.sleep(0.05)

        with self._lock:
            self._subscribed = new_symbols

        logger.info(
            f"Bybit resubscribe: +{len(to_add)} -{len(to_remove)}, "
            f"итого {len(new_symbols)}"
        )

    def stop(self):
        self._stop.set()


_client: BybitWSClient | None = None


def start_bybit_ws():
    global _client
    _client = BybitWSClient()
    try:
        asyncio.run(_client.run())
    except KeyboardInterrupt:
        _client.stop()


def bybit_resubscribe():
    if _client is not None:
        _client.request_resubscribe()


def stop_bybit_ws():
    if _client is None:
        return
    _client.stop()
    try:
        if _client._ws is not None and _client._loop is not None:
            asyncio.run_coroutine_threadsafe(
                _client._ws.close(), _client._loop
            )
    except Exception:
        pass