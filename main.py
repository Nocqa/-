# main.py
"""
Арбитражный бот: Bybit + Gate + MEXC + KuCoin через WebSocket
с динамическим списком пар.

Арбитраж считается по VWAP из локальных стаканов.

Отправка в Telegram:
  - только сигналы с net_pct >= TELEGRAM_MIN_NET_PCT (по умолчанию 0.5%)
  - emoji: 💰 (0.5..1.5%) и 🚀 (>=1.5%)
  - 📊 (слабые) идут только в stats/signals.log и в лог бота (INFO)

ПАТЧ #5: при завершении вызываются stop_*_ws() для корректной
остановки WS-клиентов (форсированное закрытие соединений).

НОВОЕ: добавлен created_at в каждый сигнал — для отсечки устаревших
сигналов в telegram_bot (см. _sender_loop).

ДИАГНОСТИКА: раз в DIAG_INTERVAL_SEC печатает состояние DIAG_SYMBOLS
по всем биржам — bid/ask, depth, valid, uid, local_age, exchange_age,
engine_age.

ФИКСЫ:
- DIAG_SYMBOLS и DIAG_INTERVAL_SEC вынесены в config.
- В _print_diag добавлены valid и uid (last_update_id) — видно,
  синхронизирован ли стакан после фиксов Gate (OBU) и MEXC (7-step).
- Добавлен счётчик "sync pending" для пар, которые ещё не синхронизированы.
"""
import asyncio
import signal
import threading
import time

from loguru import logger

from exchanges.bybit_ws import start_bybit_ws, bybit_resubscribe, stop_bybit_ws
from exchanges.gate_ws import start_gate_ws, gate_resubscribe, stop_gate_ws
from exchanges.mexc_ws import start_mexc_ws, mexc_resubscribe, stop_mexc_ws
from exchanges.kucoin_ws import start_kucoin_ws, kucoin_resubscribe, stop_kucoin_ws

from core.orderbook import orderbook_store
from core.vwap_arbitrage import (
    find_vwap_opportunities,
    compute_vwap_arbitrage,
    compute_multi_size,
)
from core.notifier import notifier
from core import active_symbols
from core.pair_discovery import discover_arbitrage_pairs
from core.signal_stats import log_signal, refresh_all_stats
from telegram_bot import start_telegram_bot, queue_signal

from config import (
    SYMBOLS, LOG_LEVEL, EXCHANGES,
    MIN_SPREAD_PCT, MIN_PROFIT_USD,
    ARBITRAGE_INTERVAL_SEC, PRINTER_INTERVAL_SEC, TRADE_VOLUME_USD,
    PAIR_REFRESH_SECONDS,
    TELEGRAM_MIN_NET_PCT,
    DIAG_SYMBOLS, DIAG_INTERVAL_SEC,
)


_shutdown = threading.Event()
STATS_REFRESH_SECONDS = 300


# --------------------------------------------------------------------- #
# Обновление списка активных пар                                        #
# --------------------------------------------------------------------- #
def pair_refresh_loop():
    logger.info(f"Pair refresh loop запущен (интервал {PAIR_REFRESH_SECONDS}s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        while not _shutdown.is_set():
            try:
                new_pairs = loop.run_until_complete(discover_arbitrage_pairs())
                if new_pairs:
                    active_symbols.update_symbols(new_pairs)
                    logger.info(f"Активных пар: {len(new_pairs)}")
                    bybit_resubscribe()
                    gate_resubscribe()
                    mexc_resubscribe()
                    kucoin_resubscribe()
                else:
                    logger.warning("Discovery вернул пустой список — оставляю прежний")
            except Exception:
                logger.exception("Pair refresh error")
            _shutdown.wait(PAIR_REFRESH_SECONDS)
    finally:
        loop.close()


# --------------------------------------------------------------------- #
# Детектор арбитража                                                    #
# --------------------------------------------------------------------- #
def arbitrage_loop():
    logger.info(
        f"Арбитражный детектор запущен "
        f"(интервал {ARBITRAGE_INTERVAL_SEC}s, "
        f"Telegram-порог net >= {TELEGRAM_MIN_NET_PCT:.3f}%)"
    )
    while not _shutdown.is_set():
        try:
            symbols = active_symbols.get_symbols()
            opps = find_vwap_opportunities(
                EXCHANGES, symbols, TRADE_VOLUME_USD, multi_size=False
            )
            for op in opps:
                op["created_at"] = time.time()

                if op["net_spread_pct"] >= TELEGRAM_MIN_NET_PCT:
                    log_fn = logger.success
                    tag = "ARB"
                else:
                    log_fn = logger.info
                    tag = "arb"

                log_fn(
                    f"{tag} {op['symbol']:10} "
                    f"{op['buy_exchange'].upper():6}->{op['sell_exchange'].upper():6} "
                    f"buy_vwap={op['buy_price']:.6g} "
                    f"sell_vwap={op['sell_price']:.6g} "
                    f"gross={op['spread_pct']:+.3f}% "
                    f"fees={op['fee_pct']:.3f}% "
                    f"slip={op['slippage_pct']:.3f}% "
                    f"net={op['net_spread_pct']:+.3f}% "
                    f"profit=+{op['net_profit_usd']:.2f}$ "
                    f"[age {op['buy_age_ms']}ms/{op['sell_age_ms']}ms "
                    f"skew {op['skew_ms']}ms]"
                )
                log_signal(op)
                queue_signal(op)
        except Exception:
            logger.exception("Arbitrage loop error")
        _shutdown.wait(ARBITRAGE_INTERVAL_SEC)


# --------------------------------------------------------------------- #
# Периодическое обновление статистики                                   #
# --------------------------------------------------------------------- #
def stats_loop():
    logger.info(f"Stats loop запущен (интервал {STATS_REFRESH_SECONDS}s)")
    try:
        refresh_all_stats()
    except Exception:
        logger.exception("Initial stats refresh error")

    while not _shutdown.is_set():
        _shutdown.wait(STATS_REFRESH_SECONDS)
        if _shutdown.is_set():
            break
        try:
            refresh_all_stats()
        except Exception:
            logger.exception("Stats refresh error")


# --------------------------------------------------------------------- #
# Печать снапшота + диагностика                                         #
# --------------------------------------------------------------------- #
def _fmt_age(ms: int) -> str:
    if ms < 0:
        return "n/a"
    if ms < 1000:
        return f"{ms}ms"
    return f"{ms/1000:.1f}s"


def _count_synced_books(symbols):
    """
    Считает по каждой бирже, сколько книг по символам из symbols
    валидны (valid=True) и сколько ещё в процессе синхронизации
    (существуют, но valid=False).
    """
    stats = {ex: {"valid": 0, "pending": 0, "missing": 0}
             for ex in EXCHANGES}
    for sym in symbols:
        for ex in EXCHANGES:
            book = orderbook_store.get(ex, sym)
            if book is None or book.is_empty():
                stats[ex]["missing"] += 1
            elif book.valid:
                stats[ex]["valid"] += 1
            else:
                stats[ex]["pending"] += 1
    return stats


def _print_diag(now: float):
    """Раз в DIAG_INTERVAL_SEC — состояние DIAG_SYMBOLS по всем биржам."""
    logger.info("[DIAG] ========== STATE ==========")
    for sym in DIAG_SYMBOLS:
        for ex in EXCHANGES:
            book = orderbook_store.get(ex, sym)
            if book is None:
                logger.info(f"[DIAG] {ex:6} {sym:10} — None")
                continue
            if book.is_empty():
                logger.info(f"[DIAG] {ex:6} {sym:10} — пустой")
                continue

            bid = book.best_bid()
            ask = book.best_ask()
            local_ms = int((now - book.ts) * 1000) if book.ts > 0 else -1
            exch_ms = book.real_age_ms(now=now)
            eng_ms = book.real_engine_age_ms(now=now)

            bid_s = f"{bid:.6g}" if bid is not None else "None"
            ask_s = f"{ask:.6g}" if ask is not None else "None"

            upd_id = book.last_update_id

            logger.info(
                f"[DIAG] {ex:6} {sym:10} "
                f"bid={bid_s:>12} ask={ask_s:>12} "
                f"depth={book.depth_bid():3d}/{book.depth_ask():3d} "
                f"valid={book.valid} "
                f"uid={upd_id} "
                f"local={_fmt_age(local_ms):>6} "
                f"exch={_fmt_age(exch_ms):>6} "
                f"eng={_fmt_age(eng_ms):>6}"
            )

    # Сводка по активным парам: сколько книг valid / pending / missing
    active = active_symbols.get_symbols()
    if active:
        stats = _count_synced_books(active)
        summary = " | ".join(
            f"{ex}: {stats[ex]['valid']}/{len(active)} valid"
            f" (+{stats[ex]['pending']} pending,"
            f" {stats[ex]['missing']} missing)"
            for ex in EXCHANGES
        )
        logger.info(f"[DIAG] SYNC {summary}")
    logger.info("[DIAG] ============================")


def printer_loop():
    last_diag = 0.0
    while not _shutdown.is_set():
        now = time.time()
        active = active_symbols.get_symbols()
        logger.info(f"--- snapshot (активных пар {len(active)}) ---")

        if now - last_diag >= DIAG_INTERVAL_SEC:
            last_diag = now
            try:
                _print_diag(now)
            except Exception as e:
                logger.exception(f"[DIAG] error: {e}")

        for sym in active:
            for ex in EXCHANGES:
                book = orderbook_store.get(ex, sym)
                if book is None or book.is_empty():
                    continue
                best_bid = book.best_bid()
                best_ask = book.best_ask()
                if best_bid is None or best_ask is None:
                    continue
                age_ms = int((now - book.ts) * 1000) if book.ts > 0 else -1
                logger.debug(
                    f"{ex:6} {sym:10} "
                    f"bid={best_bid:>12.6g} ask={best_ask:>12.6g} "
                    f"age={_fmt_age(age_ms):>6}"
                )

            for buy_ex in EXCHANGES:
                for sell_ex in EXCHANGES:
                    if buy_ex == sell_ex:
                        continue

                    arb = compute_vwap_arbitrage(
                        sym, buy_ex, sell_ex, TRADE_VOLUME_USD, now=now
                    )
                    if arb is None:
                        continue

                    if (
                        arb["net_spread_pct"] >= MIN_SPREAD_PCT
                        and arb["net_profit_usd"] >= MIN_PROFIT_USD
                    ):
                        status = "✅ ARB"
                    elif arb["net_spread_pct"] > 0:
                        status = "near"
                    else:
                        status = "—"

                    if status in ("✅ ARB", "near"):
                        logger.info(
                            f"  {sym} {buy_ex.upper():5}→{sell_ex.upper():5} "
                            f"buy_vwap={arb['buy_price']:>10.6g} "
                            f"sell_vwap={arb['sell_price']:>10.6g} "
                            f"gross={arb['spread_pct']:+.4f}% "
                            f"fees={arb['fee_pct']:.4f}% "
                            f"slip={arb['slippage_pct']:.4f}% "
                            f"NET={arb['net_spread_pct']:+.4f}% "
                            f"profit={arb['net_profit_usd']:+.2f}$ "
                            f"[age {arb['buy_age_ms']}/{arb['sell_age_ms']}ms "
                            f"skew {arb['skew_ms']}ms] "
                            f"{status}"
                        )

        _shutdown.wait(PRINTER_INTERVAL_SEC)


# --------------------------------------------------------------------- #
# Мягкая остановка                                                      #
# --------------------------------------------------------------------- #
def _handle_sigint(signum, frame):
    logger.warning("Ctrl+C — остановка")
    _shutdown.set()


# --------------------------------------------------------------------- #
# Точка входа                                                           #
# --------------------------------------------------------------------- #
if __name__ == "__main__":
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level=LOG_LEVEL, colorize=True)
    logger.add("logs/bot.log", rotation="10 MB", retention="7 days", level="DEBUG")

    signal.signal(signal.SIGINT, _handle_sigint)

    active_symbols.update_symbols(SYMBOLS)

    start_telegram_bot()
    notifier.send_status(
        "✅ Арбитражный бот запущен (VWAP)\n"
        f"Биржи: {', '.join(EXCHANGES)}\n"
        f"Объём сделки: ${TRADE_VOLUME_USD:.0f}\n"
        f"Telegram-порог: net ≥ {TELEGRAM_MIN_NET_PCT:.2f}% (💰/🚀)\n"
        f"Fallback символов: {len(SYMBOLS)}"
    )

    threading.Thread(target=start_bybit_ws, daemon=True, name="bybit").start()
    threading.Thread(target=start_gate_ws, daemon=True, name="gate").start()
    threading.Thread(target=start_mexc_ws, daemon=True, name="mexc").start()
    threading.Thread(target=start_kucoin_ws, daemon=True, name="kucoin").start()

    threading.Thread(target=pair_refresh_loop, daemon=True, name="pairrefresh").start()
    threading.Thread(target=arbitrage_loop, daemon=True, name="arb").start()
    threading.Thread(target=printer_loop, daemon=True, name="print").start()
    threading.Thread(target=stats_loop, daemon=True, name="stats").start()

    logger.info("Бот работает. Ctrl+C — остановка.")

    while not _shutdown.is_set():
        time.sleep(1)

    logger.info("Завершение работы...")

    for stop_fn, name in [
        (stop_bybit_ws, "bybit"),
        (stop_gate_ws, "gate"),
        (stop_mexc_ws, "mexc"),
        (stop_kucoin_ws, "kucoin"),
    ]:
        try:
            stop_fn()
            logger.debug(f"{name} WS остановлен")
        except Exception as e:
            logger.warning(f"{name} stop error: {e}")

    time.sleep(2)

    try:
        refresh_all_stats()
    except Exception:
        logger.exception("Final stats refresh error")
    try:
        notifier.send_status("🛑 Арбитражный бот остановлен")
    except Exception:
        pass

    logger.info("Готово.")