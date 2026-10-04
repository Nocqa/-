# exchanges/gate_ws.py
"""
Gate Spot WebSocket — Order Book V2 (spot.obu).

КЛЮЧЕВОЕ ПРЕИМУЩЕСТВО OBU:
- Первое сообщение после подписки — ПОЛНЫЙ снапшот (full=true).
- REST-синхронизация НЕ НУЖНА.
- Нет кэша уведомлений до snapshot.
- Проще и быстрее.

Формат подписки:
    payload: ["ob.<SYMBOL>.<level>"]
    level=50  → 20ms
    level=400 → 100ms

Формат сообщения (full=true):
    {
      "channel": "spot.obu",
      "result": {
        "t": <ms>,
        "full": true,
        "s": "ob.BTC_USDT.50",
        "u": <id>,
        "b": [["price","amount"], ...],
        "a": [["price","amount"], ...]
      }
    }

Формат сообщения (full=false):
    {
      "channel": "spot.obu",
      "result": {
        "t": <ms>,
        "s": "ob.BTC_USDT.50",
        "U": <start_id>,
        "u": <end_id>,
        "b": [...], "a": [...]
      }
    }

Алгоритм:
1. Первое сообщение (full=true) → set_snapshot, локальный id = u.
2. Последующие (full=false) → проверяем U == local_id + 1.
3. При GAP → unsubscribe + subscribe той же пары (per docs).

Payload — ТОЛЬКО ОДНА ПАРА НА ЗАПРОС.
Не батчить (Gate это молча принимает, но не подписывает).
"""
import json
import threading
import time

from websocket import WebSocketApp
from loguru import logger

from config import (
    WS_URLS, ORDERBOOK_DEPTH,
    GATE_USE_OBU, GATE_OBU_LEVEL,
    GATE_DEBUG_RAW, GATE_SUBSCRIBE_PAUSE,
)
from core.orderbook import orderbook_store
from core import active_symbols


RECONNECT_DELAY_SEC = 5.0


def _to_gate_symbol(symbol: str) -> str:
    return symbol.replace("-", "_")


def _from_gate_symbol(symbol: str) -> str:
    return symbol.replace("_", "-")


def _obu_stream_name(symbol: str, level: str) -> str:
    """ob.BTC_USDT.50"""
    return f"ob.{_to_gate_symbol(symbol)}.{level}"


def _parse_obu_symbol(stream_name: str) -> str | None:
    """
    'ob.BTC_USDT.50' → 'BTC-USDT'
    """
    if not stream_name or not stream_name.startswith("ob."):
        return None
    parts = stream_name.split(".")
    # ["ob", "BTC_USDT", "50"]
    if len(parts) < 3:
        return None
    return _from_gate_symbol(parts[1])


class GateWSClient:
    def __init__(self):
        self._ws = None
        self._lock = threading.Lock()
        self._subscribed = set()
        self._stop = threading.Event()

        # Локальный depth ID по паре (последний применённый u)
        self._local_id: dict[str, int] = {}
        # Счётчик последовательных GAP по паре
        self._gap_count: dict[str, int] = {}

        self._raw_count = 0

        # Уровень OBU: 50 (20ms) или 400 (100ms)
        self._obu_level = GATE_OBU_LEVEL or "50"

    # ------------------------------------------------------------------ #
    # Подписка                                                           #
    # ------------------------------------------------------------------ #
    def _subscribe_one(self, ws, symbol: str, event: str = "subscribe"):
        """Отправить subscribe/unsubscribe для ОДНОЙ пары."""
        stream = _obu_stream_name(symbol, self._obu_level)
        payload = {
            "time": int(time.time()),
            "channel": "spot.obu",
            "event": event,
            "payload": [stream],
        }
        try:
            ws.send(json.dumps(payload))
        except Exception as e:
            logger.error(f"Gate {event} send error {symbol}: {e}")
            return False
        return True

    def _subscribe_ws(self, ws):
        """Подписка на все активные пары (по одной)."""
        symbols = active_symbols.get_symbols()
        if not symbols:
            logger.warning("Gate: active_symbols пуст")
            return

        sent = 0
        for sym in symbols:
            if self._stop.is_set():
                return
            if self._subscribe_one(ws, sym, "subscribe"):
                sent += 1
            time.sleep(GATE_SUBSCRIBE_PAUSE)

        with self._lock:
            self._subscribed = set(symbols)
        logger.info(
            f"Gate subscribed via OBU ({self._obu_level}): "
            f"{sent}/{len(symbols)} пар"
        )

    # ------------------------------------------------------------------ #
    # Обработка сообщений                                                #
    # ------------------------------------------------------------------ #
    def on_message(self, ws, message):
        try:
            msg = json.loads(message)
        except Exception:
            return

        if GATE_DEBUG_RAW:
            self._raw_count += 1
            if self._raw_count <= 60:
                event = msg.get("event")
                channel = msg.get("channel")
                error = msg.get("error")
                result = msg.get("result") or {}
                extra = ""
                if event == "update" or channel == "spot.obu":
                    extra = (
                        f" full={result.get('full')} "
                        f"s={result.get('s')} "
                        f"U={result.get('U')} u={result.get('u')}"
                    )
                logger.info(
                    f"Gate RAW #{self._raw_count}: event={event} "
                    f"channel={channel} error={error}{extra}"
                )

        channel = msg.get("channel")
        event = msg.get("event")

        # Ответы на subscribe/unsubscribe
        if channel == "spot.obu" and event in ("subscribe", "unsubscribe"):
            error = msg.get("error")
            result = msg.get("result") or {}
            if error:
                logger.warning(f"Gate {event} error: {error}")
            elif result.get("status") == "success":
                logger.debug(f"Gate {event} ok: {msg.get('payload')}")
            return

        if channel != "spot.obu":
            return

        if event != "update":
            return

        result = msg.get("result") or {}
        stream_name = result.get("s")
        if not stream_name:
            return

        symbol = _parse_obu_symbol(stream_name)
        if not symbol:
            return

        is_full = bool(result.get("full", False))
        U = result.get("U")
        u = result.get("u")
        t_ms = result.get("t")
        bids = result.get("b") or []
        asks = result.get("a") or []

        book = orderbook_store.get_or_create("gate", symbol)
        exchange_ts = (t_ms / 1000.0) if t_ms else 0.0

        # ---- FULL SNAPSHOT ----
        if is_full:
            try:
                book.set_snapshot(
                    [(float(p), float(q)) for p, q in bids],
                    [(float(p), float(q)) for p, q in asks],
                    update_id=u or 0,
                    exchange_ts=exchange_ts,
                )
            except (TypeError, ValueError):
                return
            if u is not None:
                self._local_id[symbol] = u
            self._gap_count[symbol] = 0
            logger.debug(
                f"Gate OBU {symbol}: full snapshot "
                f"({len(bids)}b/{len(asks)}a, u={u})"
            )
            return

        # ---- DELTA ----
        if U is None or u is None:
            return

        local_id = self._local_id.get(symbol)
        if local_id is None:
            # Ещё не было snapshot — ждём (по докам первый всегда full)
            logger.debug(
                f"Gate OBU {symbol}: delta before snapshot, skip"
            )
            return

        # Проверка непрерывности
        if U != local_id + 1:
            self._gap_count[symbol] = self._gap_count.get(symbol, 0) + 1
            logger.warning(
                f"Gate OBU {symbol}: GAP (U={U}, local={local_id}, "
                f"count={self._gap_count[symbol]}) — resubscribe"
            )
            self._handle_gap(symbol)
            return

        # Применяем дельты
        try:
            if bids:
                book.apply_deltas(
                    "bid", [(float(p), float(q)) for p, q in bids]
                )
            if asks:
                book.apply_deltas(
                    "ask", [(float(p), float(q)) for p, q in asks]
                )
            book.update_meta(exchange_ts=exchange_ts, update_id=u)
            self._local_id[symbol] = u
            self._gap_count[symbol] = 0
        except (TypeError, ValueError):
            return

    def _handle_gap(self, symbol: str):
        """
        По документации Gate: при GAP нужно unsubscribe + subscribe.
        """
        ws = self._ws
        if ws is None:
            return

        # Сбрасываем локальный стейт
        self._local_id.pop(symbol, None)

        def _do_resub():
            try:
                self._subscribe_one(ws, symbol, "unsubscribe")
                time.sleep(0.1)
                self._subscribe_one(ws, symbol, "subscribe")
            except Exception as e:
                logger.warning(f"Gate resub {symbol} error: {e}")

        threading.Thread(
            target=_do_resub, daemon=True, name=f"gate-gap-{symbol}"
        ).start()

    def on_open(self, ws):
        logger.success("Gate WS connected (OBU)")
        with self._lock:
            self._subscribed.clear()
        self._local_id.clear()
        self._gap_count.clear()
        self._raw_count = 0

        self._subscribe_ws(ws)

    def resubscribe(self):
        """Подписка/отписка при смене active_symbols."""
        ws = self._ws
        if ws is None or not ws.sock or not ws.sock.connected:
            logger.warning("Gate resubscribe: WS не подключён")
            return

        new_symbols = set(active_symbols.get_symbols())
        with self._lock:
            old_symbols = set(self._subscribed)

        to_add = list(new_symbols - old_symbols)
        to_remove = list(old_symbols - new_symbols)

        if not to_add and not to_remove:
            return

        for sym in to_add:
            if self._stop.is_set():
                return
            if self._subscribe_one(ws, sym, "subscribe"):
                time.sleep(GATE_SUBSCRIBE_PAUSE)
        if to_add:
            logger.info(f"Gate +подписка (OBU): {len(to_add)}")

        for sym in to_remove:
            if self._stop.is_set():
                return
            self._subscribe_one(ws, sym, "unsubscribe")
            orderbook_store.purge("gate", sym)
            self._local_id.pop(sym, None)
            self._gap_count.pop(sym, None)
            time.sleep(GATE_SUBSCRIBE_PAUSE)
        if to_remove:
            logger.info(f"Gate -отписка (OBU): {len(to_remove)}")

        with self._lock:
            self._subscribed = new_symbols

    def on_error(self, ws, error):
        logger.error(f"Gate WS error: {error}")

    def on_close(self, ws, code, msg):
        logger.warning(f"Gate WS closed: code={code} msg={msg}")

    def run(self):
        while not self._stop.is_set():
            try:
                self._ws = WebSocketApp(
                    WS_URLS["gate"],
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self._ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                )
            except Exception as e:
                logger.exception(f"Gate run_forever error: {e}")

            if self._stop.is_set():
                break

            logger.warning(
                f"Gate reconnect через {RECONNECT_DELAY_SEC:.0f}s"
            )
            for _ in range(int(RECONNECT_DELAY_SEC * 2)):
                if self._stop.is_set():
                    break
                time.sleep(0.5)

    def stop(self):
        self._stop.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass


_client: GateWSClient | None = None


def start_gate_ws():
    global _client
    if not GATE_USE_OBU:
        logger.warning(
            "GATE_USE_OBU=false — этот файл поддерживает только OBU. "
            "Используйте старую версию gate_ws.py для order_book_update."
        )
    _client = GateWSClient()
    try:
        _client.run()
    except KeyboardInterrupt:
        _client.stop()


def gate_resubscribe():
    if _client is not None:
        _client.resubscribe()


def stop_gate_ws():
    if _client is not None:
        _client.stop()