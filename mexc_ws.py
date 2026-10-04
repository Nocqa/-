# exchanges/mexc_ws.py
"""
MEXC Spot WebSocket — depth (protobuf).

Канал: spot@public.aggre.depth.v3.api.pb@100ms@<SYMBOL>

7-шаговая синхронизация (по документации MEXC):
1. Подписаться на WS.
2. Кэшировать pushes, записать fromVersion первого.
3. Запросить REST /api/v3/depth?limit=5000 → lastUpdateId.
4. Если lastUpdateId < fromVersion первого push — повторить REST.
5. Отбросить pushes с toVersion <= lastUpdateId.
6. Первый оставшийся push: если fromVersion > lastUpdateId+1 → повтор REST.
7. Применить snapshot, local_version = lastUpdateId.
   Затем применяем cached pushes по порядку:
   - первого: применяем, если fromVersion <= lastUpdateId+1 (пересечение).
   - последующих: строго fromVersion == previous_toVersion + 1.
   Если нарушено — переинициализация.

Также:
- PING каждые 30 сек (MEXC требует application-level ping).
- 24-часовой лимит сессии.

ФИКСЫ:
- len(cache) может быть None → безопасная проверка.
- Первый push после REST проверяется мягко (пересечение разрешено),
  а не строго fv == local+1.
- При неудаче sync книга не остаётся valid=True от неполного snapshot.
"""
import asyncio
import json
import threading
import time
from collections import deque

import aiohttp
import websockets
from loguru import logger

from config import ORDERBOOK_DEPTH, REST_SNAPSHOT_ENABLED
from core.orderbook import orderbook_store
from core import active_symbols

try:
    from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper
except ImportError as e:
    logger.error(
        "Не найден PushDataV3ApiWrapper_pb2. "
        "Убедитесь, что файл лежит в корне проекта или в PYTHONPATH."
    )
    raise e


MEXC_WS_URL = "wss://wbs-api.mexc.com/ws"
MEXC_REST_DEPTH = "https://api.mexc.com/api/v3/depth"

SUBSCRIPTION_TEMPLATE = "spot@public.aggre.depth.v3.api.pb@100ms@{symbol}"

# REST snapshot: 5000 уровней — надёжнее для синхронизации.
MEXC_REST_LIMIT = 5000

# Сколько WS-уведомлений кэшировать до snapshot
WS_CACHE_SIZE = 500

# PING каждые N секунд
PING_INTERVAL_SEC = 30.0

# Лимит сессии (24 часа), с запасом — 23 часа
MAX_SESSION_SEC = 23 * 60 * 60

# Сколько раз повторить REST snapshot при неудачной синхронизации
MAX_RESYNC_ATTEMPTS = 8

# Ретраи REST запроса
REST_FETCH_ATTEMPTS = 3


def _to_mexc_symbol(symbol: str) -> str:
    """BTC-USDT → BTCUSDT"""
    return symbol.replace("-", "").upper()


def _from_mexc_symbol(symbol: str) -> str:
    """BTCUSDT → BTC-USDT"""
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if symbol.endswith(quote):
            return f"{symbol[:-len(quote)]}-{quote}"
    return symbol


async def _fetch_snapshot(
    session: aiohttp.ClientSession,
    symbol: str,
    attempts: int = REST_FETCH_ATTEMPTS,
):
    """REST-снимок стакана MEXC с limit=5000."""
    params = {
        "symbol": _to_mexc_symbol(symbol),
        "limit": MEXC_REST_LIMIT,
    }
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            async with session.get(
                MEXC_REST_DEPTH, params=params, timeout=15
            ) as r:
                if r.status == 429:
                    last_err = "429 rate limit"
                    await asyncio.sleep(1.0 * attempt)
                    continue
                data = await r.json()
                if isinstance(data, dict) and data.get("bids") is not None:
                    return data
                last_err = f"bad response: {str(data)[:150]}"
        except Exception as e:
            last_err = str(e)
        if attempt < attempts:
            await asyncio.sleep(0.5 * attempt)

    logger.warning(f"MEXC REST snapshot error {symbol}: {last_err}")
    return None


class MexcWSClient:
    def __init__(self):
        self._stop = threading.Event()
        self._reconnect_delay = 2.0
        self._ws = None
        self._loop = None
        self._lock = threading.Lock()
        self._subscribed = set()

        # Стейт синхронизации по каждой паре
        self._local_version: dict[str, int] = {}
        self._synced: dict[str, bool] = {}
        self._cache: dict[str, deque] = {}

        self._session_started_at = 0.0

    # ------------------------------------------------------------------ #
    # Основной цикл                                                      #
    # ------------------------------------------------------------------ #
    async def run(self):
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"MEXC WS error: {e}")

            if self._stop.is_set():
                break

            delay = self._reconnect_delay
            self._reconnect_delay = min(self._reconnect_delay * 1.5, 30.0)
            logger.warning(f"MEXC reconnect через {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        logger.info(f"MEXC connecting to {MEXC_WS_URL}")
        async with websockets.connect(
            MEXC_WS_URL,
            max_size=32 * 1024 * 1024,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._loop = asyncio.get_running_loop()
            logger.success("MEXC WS connected")
            self._reconnect_delay = 2.0
            self._session_started_at = time.time()

            # Сбрасываем стейт
            self._local_version.clear()
            self._synced.clear()
            self._cache.clear()

            symbols = active_symbols.get_symbols()
            if not symbols:
                logger.warning("MEXC: active_symbols пуст")
                return

            # ШАГ 1: подписка (до REST!)
            for sym in symbols:
                if self._stop.is_set():
                    return
                sub = {
                    "method": "SUBSCRIPTION",
                    "params": [SUBSCRIPTION_TEMPLATE.format(
                        symbol=_to_mexc_symbol(sym)
                    )],
                }
                try:
                    await ws.send(json.dumps(sub))
                except Exception as e:
                    logger.error(f"MEXC subscribe error {sym}: {e}")
                await asyncio.sleep(0.03)

            with self._lock:
                self._subscribed = set(symbols)
            logger.info(f"MEXC подписан на {len(symbols)} пар (depth)")

            # Инициализируем cache для каждой пары
            for sym in symbols:
                self._cache[sym] = deque(maxlen=WS_CACHE_SIZE)
                self._synced[sym] = False

            sync_task = asyncio.create_task(self._sync_all_symbols(symbols))
            ping_task = asyncio.create_task(self._ping_loop(ws))
            session_task = asyncio.create_task(self._session_timer(ws))

            try:
                async for message in ws:
                    try:
                        self._handle_message(message)
                    except Exception as e:
                        logger.exception(f"MEXC message error: {e}")
            finally:
                for t in (sync_task, ping_task, session_task):
                    t.cancel()

    # ------------------------------------------------------------------ #
    # PING / сессия                                                      #
    # ------------------------------------------------------------------ #
    async def _ping_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL_SEC)
                try:
                    await ws.send(json.dumps({"method": "PING"}))
                except Exception as e:
                    logger.warning(f"MEXC ping error: {e}")
                    return
        except asyncio.CancelledError:
            return

    async def _session_timer(self, ws):
        try:
            await asyncio.sleep(MAX_SESSION_SEC)
            logger.info("MEXC session timer: переподключение")
            try:
                await ws.close()
            except Exception:
                pass
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------------ #
    # Синхронизация                                                      #
    # ------------------------------------------------------------------ #
    async def _sync_all_symbols(self, symbols):
        async with aiohttp.ClientSession() as session:
            for sym in symbols:
                if self._stop.is_set():
                    return
                try:
                    await self._sync_one_symbol(session, sym)
                except Exception as e:
                    logger.exception(f"MEXC sync error {sym}: {e}")
                await asyncio.sleep(0.05)

        ok = sum(1 for s in symbols if self._synced.get(s, False))
        logger.info(f"MEXC sync complete: {ok}/{len(symbols)} пар")

    async def _sync_one_symbol(self, session, symbol):
        """
        7-шаговая синхронизация одной пары.
        При неудаче — пара НЕ помечается valid (book.mark_stale()).
        """
        for attempt in range(1, MAX_RESYNC_ATTEMPTS + 1):
            if self._stop.is_set():
                return

            # Гарантируем наличие cache
            cache = self._cache.get(symbol)
            if cache is None:
                cache = deque(maxlen=WS_CACHE_SIZE)
                self._cache[symbol] = cache

            data = await _fetch_snapshot(session, symbol)
            if not data:
                logger.warning(
                    f"MEXC {symbol}: REST snapshot failed "
                    f"(attempt {attempt}/{MAX_RESYNC_ATTEMPTS})"
                )
                await asyncio.sleep(0.5 * attempt)
                continue

            bids = data.get("bids") or []
            asks = data.get("asks") or []
            try:
                last_update_id = int(data.get("lastUpdateId", 0))
            except (TypeError, ValueError):
                last_update_id = 0

            if last_update_id <= 0:
                logger.warning(
                    f"MEXC {symbol}: bad lastUpdateId={last_update_id}"
                )
                await asyncio.sleep(0.5 * attempt)
                continue

            # ШАГ 4: если REST устарел относительно первого push — повтор
            if cache and len(cache) > 0:
                first = cache[0]
                first_from = first["from_version"]
                if first_from > 0 and last_update_id < first_from:
                    logger.warning(
                        f"MEXC {symbol}: REST stale "
                        f"(lastUpdateId={last_update_id} < "
                        f"first_from={first_from}), retry"
                    )
                    await asyncio.sleep(0.3)
                    continue

            # Применяем snapshot
            book = orderbook_store.get_or_create("mexc", symbol)
            try:
                book.set_snapshot(
                    [(float(p), float(q)) for p, q in bids],
                    [(float(p), float(q)) for p, q in asks],
                    update_id=last_update_id,
                )
            except (TypeError, ValueError) as e:
                logger.warning(f"MEXC {symbol}: bad snapshot data: {e}")
                await asyncio.sleep(0.5 * attempt)
                continue

            self._local_version[symbol] = last_update_id

            # ШАГ 5-7: применяем кэш
            applied = 0
            gap_detected = False
            first_applied = False

            while cache:
                notif = cache.popleft()
                fv = notif["from_version"]
                tv = notif["to_version"]

                # Отбрасываем полностью устаревшие
                if tv <= last_update_id:
                    continue

                # ПЕРВЫЙ push: мягкая проверка — он должен пересекаться
                # с snapshot (fv <= last_update_id + 1 <= tv).
                # Если fv > last_update_id + 1 — реальный разрыв.
                if not first_applied:
                    if fv > last_update_id + 1:
                        logger.warning(
                            f"MEXC {symbol}: gap after snapshot "
                            f"(fv={fv} > last+1={last_update_id+1})"
                        )
                        gap_detected = True
                        break
                    # Применяем, не проверяя строго fv == local+1,
                    # т.к. пересечение с REST допустимо.
                    first_applied = True
                else:
                    # Последующие — строгая непрерывность
                    if fv != self._local_version[symbol] + 1:
                        logger.warning(
                            f"MEXC {symbol}: discontinuity "
                            f"(fv={fv} != local+1={self._local_version[symbol]+1})"
                        )
                        gap_detected = True
                        break

                try:
                    if notif["bids"]:
                        book.apply_deltas(
                            "bid",
                            [(float(p), float(q))
                             for p, q in notif["bids"]],
                        )
                    if notif["asks"]:
                        book.apply_deltas(
                            "ask",
                            [(float(p), float(q))
                             for p, q in notif["asks"]],
                        )
                    self._local_version[symbol] = tv
                    applied += 1
                except (TypeError, ValueError):
                    gap_detected = True
                    break

            if gap_detected:
                logger.warning(
                    f"MEXC {symbol}: reinit "
                    f"(attempt {attempt}/{MAX_RESYNC_ATTEMPTS})"
                )
                # ВАЖНО: помечаем книгу stale, чтобы не торговать на ней
                book.mark_stale()
                self._cache[symbol] = deque(maxlen=WS_CACHE_SIZE)
                await asyncio.sleep(0.3)
                continue

            # Успех
            self._synced[symbol] = True
            logger.info(
                f"MEXC {symbol}: synced "
                f"(lastUpdateId={last_update_id}, "
                f"applied={applied} cached, "
                f"cache_rest={len(cache) if cache is not None else 0})"
            )
            return

        # После MAX_RESYNC_ATTEMPTS
        logger.error(
            f"MEXC {symbol}: sync failed after "
            f"{MAX_RESYNC_ATTEMPTS} attempts — pair not tradeable"
        )
        self._synced[symbol] = False
        # Книга может быть в промежуточном состоянии — помечаем stale
        book = orderbook_store.get("mexc", symbol)
        if book is not None:
            book.mark_stale()

    # ------------------------------------------------------------------ #
    # Обработка сообщений                                                #
    # ------------------------------------------------------------------ #
    def _handle_message(self, message):
        if isinstance(message, str):
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                return
            code = data.get("code")
            if code not in (None, 0):
                logger.warning(f"MEXC response: {data}")
            else:
                logger.debug(f"MEXC json: {data}")
            return

        if not isinstance(message, (bytes, bytearray)):
            return

        wrapper = PushDataV3ApiWrapper()
        try:
            wrapper.ParseFromString(message)
        except Exception as e:
            logger.debug(f"MEXC parse error: {e}")
            return

        if wrapper.HasField("publicAggreDepths"):
            self._handle_depth(wrapper)
            return

    def _handle_depth(self, wrapper: PushDataV3ApiWrapper):
        depths = wrapper.publicAggreDepths
        symbol_raw = (wrapper.symbol or "").upper()
        if not symbol_raw:
            return

        symbol = _from_mexc_symbol(symbol_raw)

        try:
            from_version = int(depths.fromVersion or 0)
            to_version = int(depths.toVersion or 0)
        except (TypeError, ValueError):
            return

        if from_version == 0 or to_version == 0:
            return

        bids = []
        for item in depths.bids:
            try:
                p = float(item.price)
                q = float(item.quantity)
            except (TypeError, ValueError):
                continue
            if p > 0:
                bids.append((p, q))
        asks = []
        for item in depths.asks:
            try:
                p = float(item.price)
                q = float(item.quantity)
            except (TypeError, ValueError):
                continue
            if p > 0:
                asks.append((p, q))

        # Не синхронизирована — кэшируем
        if not self._synced.get(symbol, False):
            cache = self._cache.get(symbol)
            if cache is None:
                cache = deque(maxlen=WS_CACHE_SIZE)
                self._cache[symbol] = cache
            cache.append({
                "from_version": from_version,
                "to_version": to_version,
                "bids": bids,
                "asks": asks,
            })
            return

        # Синхронизирована — строгая проверка непрерывности
        local = self._local_version.get(symbol, 0)
        if from_version != local + 1:
            logger.warning(
                f"MEXC {symbol}: runtime gap "
                f"(fv={from_version}, local+1={local + 1}) — reinit"
            )
            book = orderbook_store.get("mexc", symbol)
            if book is not None:
                book.mark_stale()
            self._synced[symbol] = False
            self._cache[symbol] = deque(maxlen=WS_CACHE_SIZE)
            self._cache[symbol].append({
                "from_version": from_version,
                "to_version": to_version,
                "bids": bids,
                "asks": asks,
            })
            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(
                    self._reinit_symbol(symbol), self._loop
                )
            return

        book = orderbook_store.get_or_create("mexc", symbol)
        try:
            if bids:
                book.apply_deltas("bid", bids)
            if asks:
                book.apply_deltas("ask", asks)
            self._local_version[symbol] = to_version
        except (TypeError, ValueError):
            return

    async def _reinit_symbol(self, symbol):
        async with aiohttp.ClientSession() as session:
            await self._sync_one_symbol(session, symbol)

    # ------------------------------------------------------------------ #
    # Resubscribe                                                        #
    # ------------------------------------------------------------------ #
    def request_resubscribe(self):
        if self._loop is None or self._ws is None:
            logger.warning("MEXC resubscribe: WS не подключён")
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
                "method": "UNSUBSCRIPTION",
                "params": [SUBSCRIPTION_TEMPLATE.format(
                    symbol=_to_mexc_symbol(sym)
                )],
            }
            try:
                await ws.send(json.dumps(msg))
            except Exception as e:
                logger.error(f"MEXC unsubscribe error {sym}: {e}")
            await asyncio.sleep(0.02)
            orderbook_store.purge("mexc", sym)
            self._local_version.pop(sym, None)
            self._synced.pop(sym, None)
            self._cache.pop(sym, None)

        for sym in to_add:
            msg = {
                "method": "SUBSCRIPTION",
                "params": [SUBSCRIPTION_TEMPLATE.format(
                    symbol=_to_mexc_symbol(sym)
                )],
            }
            try:
                await ws.send(json.dumps(msg))
            except Exception as e:
                logger.error(f"MEXC subscribe error {sym}: {e}")
            await asyncio.sleep(0.02)
            self._cache[sym] = deque(maxlen=WS_CACHE_SIZE)
            self._synced[sym] = False

        with self._lock:
            self._subscribed = new_symbols

        if to_add:
            asyncio.create_task(self._sync_all_symbols(list(to_add)))

        logger.info(
            f"MEXC resubscribe: +{len(to_add)} -{len(to_remove)}, "
            f"итого {len(new_symbols)}"
        )

    def stop(self):
        self._stop.set()


_client: MexcWSClient | None = None


def start_mexc_ws():
    global _client
    _client = MexcWSClient()
    try:
        asyncio.run(_client.run())
    except KeyboardInterrupt:
        _client.stop()


def mexc_resubscribe():
    if _client is not None:
        _client.request_resubscribe()


def stop_mexc_ws():
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