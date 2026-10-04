# telegram_bot.py
"""
Telegram-модуль арбитражного бота.

Отправка сигналов: см. core/notifier.py — там фильтр по net_pct.
Здесь только очередь и команды.

НОВОЕ:
1. Отсечка устаревших сигналов (age > 30 сек) — не отправляем.
2. REST-верификация для 🚀 (net >= 3%) — сверяем WS с REST.
"""
import asyncio
import json
import queue
import threading
import time
import requests
from loguru import logger

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_COOLDOWN_SEC,
    TELEGRAM_MIN_NET_PCT,
)
from core.notifier import notifier
from core.verifier import verify_signal


send_queue: "queue.Queue[dict]" = queue.Queue(maxsize=1000)

_running = True
_last_signal: dict | None = None
_started_at = time.time()

_stats = {
    "signals_sent": 0,
    "signals_skipped": 0,
    "signals_filtered": 0,
    "signals_stale": 0,      # <<< НОВОЕ: устаревшие по времени
    "signals_failed_rest": 0, # <<< НОВОЕ: не прошли REST-верификацию
}

# <<< НОВОЕ: параметры отсечки
MAX_SIGNAL_AGE_SEC = 30.0        # сигнал старше 30 сек — не отправлять
REST_VERIFY_MIN_NET_PCT = 3.0    # для 🚀 (net >= 3%) — проверять через REST


def queue_signal(signal: dict) -> None:
    try:
        send_queue.put_nowait(signal)
    except queue.Full:
        _stats["signals_skipped"] += 1
        logger.warning("Telegram send_queue переполнена")


def start_telegram_bot() -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram не настроен: проверьте .env")
        return

    threading.Thread(target=_sender_loop, daemon=True, name="tg-sender").start()
    threading.Thread(target=_command_loop, daemon=True, name="tg-cmd").start()
    logger.success("Telegram-модуль запущен")


def _sender_loop():
    global _last_signal
    while _running:
        try:
            sig = send_queue.get(timeout=1)
        except queue.Empty:
            continue

        try:
            net_pct = float(sig.get("net_spread_pct", 0))

            # <<< ДОБАВЛЕНО: отсечка по времени жизни сигнала
            created_at = sig.get("created_at", 0)
            age_sec = time.time() - created_at if created_at > 0 else 0
            if age_sec > MAX_SIGNAL_AGE_SEC:
                _stats["signals_stale"] += 1
                logger.warning(
                    f"Сигнал устарел ({age_sec:.1f}s): "
                    f"{sig.get('symbol')} "
                    f"{sig.get('buy_exchange')}->{sig.get('sell_exchange')}"
                )
                continue

            # <<< ДОБАВЛЕНО: REST-верификация для 🚀
            if net_pct >= REST_VERIFY_MIN_NET_PCT:
                try:
                    verified = asyncio.run(verify_signal(sig))
                    if not verified:
                        _stats["signals_failed_rest"] += 1
                        logger.warning(
                            f"Сигнал отфильтрован REST-верификацией: "
                            f"{sig.get('symbol')} "
                            f"{sig.get('buy_exchange')}->{sig.get('sell_exchange')} "
                            f"net={net_pct:+.3f}%"
                        )
                        continue
                except Exception as e:
                    _stats["signals_failed_rest"] += 1
                    logger.warning(
                        f"REST verify error для {sig.get('symbol')}: {e} — "
                        f"пропускаем 🚀"
                    )
                    continue

            ok = notifier.send_arbitrage(
                symbol=sig.get("symbol", "?"),
                buy_exchange=sig.get("buy_exchange", "?"),
                sell_exchange=sig.get("sell_exchange", "?"),
                buy_price=float(sig.get("buy_price", 0)),
                sell_price=float(sig.get("sell_price", 0)),
                gross_pct=float(sig.get("spread_pct", 0)),
                fee_pct=float(sig.get("fee_pct", 0)),
                slippage_pct=float(sig.get("slippage_pct", 0)),
                net_pct=net_pct,
                net_profit_usd=float(sig.get("net_profit_usd", 0)),
                volume_usd=float(sig.get("volume_usd", 200)),
                age_buy_ms=int(sig.get("buy_age_ms", 0)),
                age_sell_ms=int(sig.get("sell_age_ms", 0)),
                buy_best=sig.get("buy_best"),
                sell_best=sig.get("sell_best"),
                buy_slip_pct=sig.get("buy_slip_pct"),
                sell_slip_pct=sig.get("sell_slip_pct"),
                sizes=sig.get("sizes"),
            )

            if ok:
                _stats["signals_sent"] += 1
                _last_signal = sig
            elif net_pct < TELEGRAM_MIN_NET_PCT:
                _stats["signals_filtered"] += 1

        except Exception as e:
            logger.exception(f"Ошибка отправки сигнала: {e}")


def _get_updates(offset: int, timeout: int = 25):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        r = requests.get(url, params={
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": json.dumps(["message"]),
        }, timeout=timeout + 5)
        if r.ok:
            return r.json().get("result", [])
    except requests.exceptions.ReadTimeout:
        pass
    except Exception as e:
        logger.warning(f"getUpdates error: {e}")
    return []


def _command_loop():
    logger.info("Telegram command loop started")
    offset = 0
    while _running:
        updates = _get_updates(offset)
        for upd in updates:
            offset = upd["update_id"] + 1
            msg = upd.get("message") or {}
            chat_id = msg.get("chat", {}).get("id")
            text = (msg.get("text") or "").strip()

            if chat_id != TELEGRAM_CHAT_ID:
                continue
            if not text.startswith("/"):
                continue

            cmd = text.split()[0].split("@")[0].lower()
            handler = _COMMANDS.get(cmd)
            if handler:
                try:
                    handler()
                except Exception as e:
                    logger.exception(f"Команда {cmd} упала: {e}")
                    notifier.send(f"⚠️ Ошибка: <code>{e}</code>")


def _send(text: str):
    return notifier.send(text)


# --------------------------------------------------------------------- #
# Команды                                                               #
# --------------------------------------------------------------------- #
def _cmd_start():
    _send(
        "🤖 <b>Arbitrage Bot (VWAP)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Слежу за спредами Gate ↔ MEXC ↔ KuCoin ↔ Bybit.\n"
        f"В Telegram шлю только 💰 и 🚀 (net ≥ {TELEGRAM_MIN_NET_PCT:.2f}%).\n"
        f"Отсечка: сигналы старше {MAX_SIGNAL_AGE_SEC:.0f}s не отправляются.\n"
        f"REST-верификация для 🚀 (net ≥ {REST_VERIFY_MIN_NET_PCT:.0f}%).\n\n"
        "/status — состояние\n"
        "/last — последний сигнал\n"
        "/stats — статистика\n"
        "/help — помощь"
    )


def _cmd_help():
    _send(
        "<b>Команды</b>\n"
        "/status — состояние бота\n"
        "/last — последний сигнал\n"
        "/stats — статистика (отправлено/пропущено/устарело/REST-fail)\n"
        "/help — эта справка"
    )


def _cmd_status():
    uptime = int(time.time() - _started_at)
    h, m = divmod(uptime // 60, 60)
    _send(
        f"🟢 <b>Статус</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Uptime: <b>{h}h {m}m</b>\n"
        f"Очередь: <b>{send_queue.qsize()}</b>\n"
        f"Отправлено сигналов: <b>{_stats['signals_sent']}</b>\n"
        f"Отфильтровано (net < {TELEGRAM_MIN_NET_PCT:.2f}%): "
        f"<b>{_stats['signals_filtered']}</b>\n"
        f"Устарело (age > {MAX_SIGNAL_AGE_SEC:.0f}s): <b>{_stats['signals_stale']}</b>\n"
        f"REST-fail (🚀): <b>{_stats['signals_failed_rest']}</b>\n"
        f"Cooldown: <b>{TELEGRAM_COOLDOWN_SEC}s</b>"
    )


def _cmd_stats():
    nc = notifier.get_counters()
    _send(
        f"📊 <b>Статистика</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Порог для Telegram: <b>net ≥ {TELEGRAM_MIN_NET_PCT:.2f}%</b>\n"
        f"Порог 🚀: <b>net ≥ {nc['rocket_net_pct']:.2f}%</b>\n"
        f"Порог REST-верификации: <b>net ≥ {REST_VERIFY_MIN_NET_PCT:.0f}%</b>\n\n"
        f"<b>Очередь бота:</b>\n"
        f"  отправлено: <b>{_stats['signals_sent']}</b>\n"
        f"  отфильтровано по net: <b>{_stats['signals_filtered']}</b>\n"
        f"  устарело (age > {MAX_SIGNAL_AGE_SEC:.0f}s): <b>{_stats['signals_stale']}</b>\n"
        f"  REST-fail: <b>{_stats['signals_failed_rest']}</b>\n"
        f"  пропущено (очередь полна): <b>{_stats['signals_skipped']}</b>\n"
        f"  очередь сейчас: <b>{send_queue.qsize()}</b>\n\n"
        f"<b>Notifier:</b>\n"
        f"  sent: <b>{nc['sent']}</b>\n"
        f"  filtered: <b>{nc['filtered']}</b>\n"
        f"  cooldown skipped: <b>{nc['cooldown_skipped']}</b>"
    )


def _cmd_last():
    if not _last_signal:
        _send("📭 Сигналов ещё не было")
        return
    s = _last_signal
    _send(
        f"📊 <b>Последний сигнал</b>\n"
        f"💎 {s.get('symbol')}\n"
        f"🟢 buy {s.get('buy_exchange','?').upper()} "
        f"VWAP {s.get('buy_price')}\n"
        f"🔴 sell {s.get('sell_exchange','?').upper()} "
        f"VWAP {s.get('sell_price')}\n"
        f"📈 gross <b>{s.get('spread_pct', 0):+.3f}%</b>\n"
        f"📉 net <b>{s.get('net_spread_pct', 0):+.3f}%</b>\n"
        f"💵 +{s.get('net_profit_usd', 0):.2f} USD"
    )


_COMMANDS = {
    "/start": _cmd_start,
    "/help": _cmd_help,
    "/status": _cmd_status,
    "/stats": _cmd_stats,
    "/last": _cmd_last,
}