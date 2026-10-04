# exchanges/kucoin_ws.py
"""
KuCoin Spot WebSocket — level2Depth50.

Схема:
1. POST /api/v1/bullet-public → token + endpoint
2. WebSocket к <endpoint>?token=<token>
3. Subscribe на /spotMarket/level2Depth50:<SYMBOL>
4. Heartbeat: ping/pong
5. Токен живёт 24 часа → переподключение

ВАЖНО:
- level2Depth50 присылает SNAPSHOT 50 уровней (не delta).
  Используем set_snapshot, а не apply_deltas.
- Сохраняем биржевой timestamp — чтобы потом считать
  реальный возраст стакана (exchange_age_ms).
"""
import asyncio
import json
import random
import string
import threading
import time

import aiohttp
import websockets
from loguru import logger

from core.orderbook import orderbook_store
from core import active_symbols


KUCOIN_REST_BASE = "https://api.kucoin.com"
KUCOIN_BULLET_URL = f"{KUCOIN_REST_BASE}/api/v1/bullet-public"
KUCOIN_MAX_SESSION_SEC = 20 * 60 * 60

KUCOIN_TOPIC_TEMPLATE = "/spotMarket/level2Depth50:{symbol}"

# Логировать exchange_age только если он больше этого порога
KUCOIN_AGE_WARN_MS = 200


def _rand_id(k: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))


def _from_kucoin_symbol(symbol: str) -> str:
    return symbol.upper()


class KuCoinWSClient:
    def __init__(self):
        self._stop = threading.Event()
        self._reconnect_delay = 2.0
        self._ws = None
        self._loop = None
        self._lock = threading.Lock()
        self._subscribed = set()

    async def _fetch_bullet(self) -> tuple[str, str]:
        async with aiohttp.ClientSession() as session:
            async with session.post(KUCOIN_BULLET_URL, timeout=15) as r:
                data = await r.json()

        if str(data.get("code", "")) != "200000":
            raise RuntimeError(f"KuCoin bullet-public code={data.get('code')}")

        payload = data.get("data", {})
        token = payload.get("token")
        servers = payload.get("instanceServers", [])
        if not token or not servers:
            raise RuntimeError("KuCoin bullet-public: пустой ответ")

        endpoint = servers[0].get("endpoint")
        if not endpoint:
            raise RuntimeError("KuCoin bullet-public: нет endpoint")

        return token, endpoint

    async def run(self):
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"KuCoin WS error: {e}")

            if self._stop.is_set():
                break

            delay = self._reconnect_delay
            self._reconnect_delay = min(self._reconnect_delay * 1.5, 30.0)
            logger.warning(f"KuCoin reconnect через {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        token, endpoint = await self._fetch_bullet()
        connect_id = _rand_id()
        url = f"{endpoint}?token={token}&connectId={connect_id}"
        logger.info(f"KuCoin connecting to {endpoint}")

        async with websockets.connect(
            url,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
            max_size=16 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            self._loop = asyncio.get_running_loop()
            logger.success("KuCoin WS connected")
            self._reconnect_delay = 2.0

            symbols = active_symbols.get_symbols()
            if not symbols:
                logger.warning("KuCoin: active_symbols пуст")
            else:
                for sym in symbols:
                    sub = {
                        "id": _rand_id(),
                        "type": "subscribe",
                        "topic": KUCOIN_TOPIC_TEMPLATE.format(symbol=sym),
                        "privateChannel": False,
                        "response": True,
                    }
                    try:
                        await ws.send(json.dumps(sub))
                    except Exception as e:
                        logger.error(f"KuCoin subscribe error {sym}: {e}")
                    await asyncio.sleep(0.02)
                with self._lock:
                    self._subscribed = set(symbols)
                logger.info(f"KuCoin подписан на {len(symbols)} пар")

            hb_task = asyncio.create_task(self._heartbeat_loop(ws))
            timer_task = asyncio.create_task(self._session_timer())

            try:
                async for message in ws:
                    try:
                        self._handle_message(message)
                    except Exception as e:
                        logger.exception(f"KuCoin message error: {e}")
            finally:
                hb_task.cancel()
                timer_task.cancel()

    async def _heartbeat_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(15)
                ping_msg = {"id": _rand_id(), "type": "ping"}
                try:
                    await ws.send(json.dumps(ping_msg))
                except Exception as e:
                    logger.warning(f"KuCoin ping error: {e}")
                    return
        except asyncio.CancelledError:
            return

    async def _session_timer(self):
        try:
            await asyncio.sleep(KUCOIN_MAX_SESSION_SEC)
            logger.info("KuCoin session timer: переподключение")
            if self._ws is not None:
                await self._ws.close()
        except asyncio.CancelledError:
            return

    def _handle_message(self, message):
        if not isinstance(message, str):
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        msg_type = data.get("type")

        if msg_type == "ping":
            try:
                pong = {"id": data.get("id", _rand_id()), "type": "pong"}
                if self._ws is not None and self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        self._ws.send(json.dumps(pong)),
                        self._loop,
                    )
            except Exception:
                pass
            return

        if msg_type in ("welcome", "ack"):
            return
        if msg_type != "message":
            return

        topic = data.get("topic", "")
        if not topic.startswith("/spotMarket/level2Depth50:"):
            return

        parts = topic.split(":", 1)
        if len(parts) < 2:
            return
        symbol_raw = parts[1].split(",")[0].strip().upper()

        payload = data.get("data") or {}
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []

        if not bids and not asks:
            return

        # <<< Кукойн timestamp в миллисекундах → секунды
        exchange_ts = 0.0
        ts_raw = payload.get("timestamp")
        if ts_raw:
            try:
                exchange_ts = int(ts_raw) / 1000.0
            except (TypeError, ValueError):
                exchange_ts = 0.0

        symbol = _from_kucoin_symbol(symbol_raw)
        book = orderbook_store.get_or_create("kucoin", symbol)

        # <<< ЗАМЕНА: level2Depth50 — это SNAPSHOT, не delta
        try:
            book.set_snapshot(
                [(float(p), float(q)) for p, q in bids],
                [(float(p), float(q)) for p, q in asks],
                exchange_ts=exchange_ts,
            )
        except (TypeError, ValueError):
            return

        # <<< ДИАГНОСТИКА: реальный возраст KuCoin
        if exchange_ts > 0:
            local_ms = int(time.time() * 1000)
            age_ms = local_ms - int(exchange_ts * 1000)
            if age_ms > KUCOIN_AGE_WARN_MS:
                logger.warning(
                    f"KUCOIN {symbol} exchange_age={age_ms}ms"
                )

    def request_resubscribe(self):
        if self._loop is None or self._ws is None:
            logger.warning("KuCoin resubscribe: WS не подключён")
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

        for sym in to_remove:
            msg = {
                "id": _rand_id(),
                "type": "unsubscribe",
                "topic": KUCOIN_TOPIC_TEMPLATE.format(symbol=sym),
                "privateChannel": False,
                "response": True,
            }
            try:
                await ws.send(json.dumps(msg))
            except Exception as e:
                logger.error(f"KuCoin unsubscribe error {sym}: {e}")
            await asyncio.sleep(0.02)
            orderbook_store.purge("kucoin", sym)

        for sym in to_add:
            msg = {
                "id": _rand_id(),
                "type": "subscribe",
                "topic": KUCOIN_TOPIC_TEMPLATE.format(symbol=sym),
                "privateChannel": False,
                "response": True,
            }
            try:
                await ws.send(json.dumps(msg))
            except Exception as e:
                logger.error(f"KuCoin subscribe error {sym}: {e}")
            await asyncio.sleep(0.02)

        with self._lock:
            self._subscribed = new_symbols

        logger.info(
            f"KuCoin resubscribe: +{len(to_add)} -{len(to_remove)}, "
            f"итого {len(new_symbols)}"
        )

    def stop(self):
        self._stop.set()


_client: KuCoinWSClient | None = None


def start_kucoin_ws():
    global _client
    _client = KuCoinWSClient()
    try:
        asyncio.run(_client.run())
    except KeyboardInterrupt:
        _client.stop()


def kucoin_resubscribe():
    if _client is not None:
        _client.request_resubscribe()


def stop_kucoin_ws():
    """Корректная остановка KuCoin WS."""
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