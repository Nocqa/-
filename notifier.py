# core/notifier.py
"""
Telegram-уведомитель с поддержкой VWAP-сигналов.

Отправляет в Telegram только "сильные" сигналы:
    net_pct >= TELEGRAM_MIN_NET_PCT  (по умолчанию 0.5%)
то есть только 💰 (0.5..1.5%) и 🚀 (>=1.5%).

Слабые сигналы (📊, net < 0.5%) в Telegram НЕ отправляются.
"""
import time
import threading
from typing import Optional
import requests
from loguru import logger

# <<< ИЗМЕНЕНО: добавлены TELEGRAM_MIN_NET_PCT, TELEGRAM_ROCKET_NET_PCT,
#              TELEGRAM_ROCKET_NO_COOLDOWN
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_COOLDOWN_SEC,
    TELEGRAM_MIN_NET_PCT,
    TELEGRAM_ROCKET_NET_PCT,
    TELEGRAM_ROCKET_NO_COOLDOWN,
)


class TelegramNotifier:
    def __init__(
        self,
        token: str = TELEGRAM_BOT_TOKEN,
        chat_id: int = TELEGRAM_CHAT_ID,
        cooldown_sec: int = TELEGRAM_COOLDOWN_SEC,
    ):
        self.token = token
        self.chat_id = chat_id
        self.cooldown_sec = cooldown_sec
        self.base_url = f"https://api.telegram.org/bot{self.token}"

        self._last_sent: dict[str, float] = {}
        self._lock = threading.Lock()

        # <<< ДОБАВЛЕНО: счётчики для /stats
        self._filtered_count = 0
        self._sent_count = 0
        self._cooldown_skipped = 0

        if not self.token:
            logger.warning("TELEGRAM_BOT_TOKEN пустой")
        if not self.chat_id:
            logger.warning("TELEGRAM_CHAT_ID = 0")

    def is_enabled(self) -> bool:
        return bool(self.token) and bool(self.chat_id)

    def send(
        self,
        text: str,
        parse_mode: str = "HTML",
        disable_notification: bool = False,
        key: Optional[str] = None,
        ignore_cooldown: bool = False,   # <<< ДОБАВЛЕНО
    ) -> bool:
        if not self.is_enabled():
            return False

        # <<< ИЗМЕНЕНО: ignore_cooldown пропускает проверку cooldown
        if key and not ignore_cooldown and self._is_in_cooldown(key):
            self._cooldown_skipped += 1
            return False

        ok = self._post_with_retries(text, parse_mode, disable_notification)
        if ok:
            self._sent_count += 1
            if key:
                with self._lock:
                    self._last_sent[key] = time.time()
        return ok

    # ------------------------------------------------------------------ #
    # VWAP-сигнал                                                        #
    # ------------------------------------------------------------------ #
    def send_arbitrage(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,           # VWAP покупки
        sell_price: float,          # VWAP продажи
        gross_pct: float,
        fee_pct: float,
        slippage_pct: float,
        net_pct: float,
        net_profit_usd: float,
        volume_usd: float = 200.0,
        age_buy_ms: int = 0,
        age_sell_ms: int = 0,
        buy_best: Optional[float] = None,
        sell_best: Optional[float] = None,
        buy_slip_pct: Optional[float] = None,
        sell_slip_pct: Optional[float] = None,
        sizes: Optional[list] = None,
    ) -> bool:
        # ------------------------------------------------------------- #
        # <<< ДОБАВЛЕНО: фильтр слабых сигналов.
        # Если net_pct ниже порога — в Telegram не отправляем.
        # ------------------------------------------------------------- #
        if net_pct < TELEGRAM_MIN_NET_PCT:
            self._filtered_count += 1
            logger.debug(
                f"Telegram: пропуск слабого сигнала "
                f"{symbol} {buy_exchange}->{sell_exchange} "
                f"net={net_pct:+.3f}% < {TELEGRAM_MIN_NET_PCT:.3f}%"
            )
            return False

        # emoji: 🚀 для сильных, 💰 для средних. 📊 больше не используется,
        # потому что слабые (< TELEGRAM_MIN_NET_PCT) отсечены выше.
        emoji = "🚀" if net_pct >= TELEGRAM_ROCKET_NET_PCT else "💰"

        # Блок best/VWAP
        buy_block = f"🟢 Купить на <b>{buy_exchange.upper()}</b>\n"
        if buy_best is not None:
            buy_block += f"   best: <code>{buy_best:.8g}</code>\n"
        buy_block += f"   VWAP: <code>{buy_price:.8g}</code>\n"
        if buy_slip_pct is not None:
            buy_block += f"   slip: <b>{buy_slip_pct:+.4f}%</b>\n"

        sell_block = f"🔴 Продать на <b>{sell_exchange.upper()}</b>\n"
        if sell_best is not None:
            sell_block += f"   best: <code>{sell_best:.8g}</code>\n"
        sell_block += f"   VWAP: <code>{sell_price:.8g}</code>\n"
        if sell_slip_pct is not None:
            sell_block += f"   slip: <b>{sell_slip_pct:+.4f}%</b>\n"

        # Мульти-объём
        sizes_block = ""
        if sizes:
            lines = ["", "📦 <b>По объёмам:</b>"]
            for s in sizes:
                v = s.get("volume")
                n = s.get("net_pct")
                p = s.get("net_profit_usd")
                if n is None:
                    lines.append(f"   ${v:.0f}: <i>нет данных</i>")
                else:
                    mark = "✅" if n > 0 else "❌"
                    lines.append(
                        f"   ${v:.0f}: NET <b>{n:+.3f}%</b> "
                        f"(${p:+.2f}) {mark}"
                    )
            sizes_block = "\n".join(lines)

        text = (
            f"{emoji} <b>Арбитражный сигнал (VWAP)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <b>{symbol}</b>\n"
            f"📦 Объём: <b>${volume_usd:.0f}</b>\n\n"
            f"{buy_block}\n"
            f"{sell_block}\n"
            f"📈 Gross: <b>{gross_pct:+.3f}%</b>\n"
            f"🧾 Fees: <b>-{fee_pct:.3f}%</b>\n"
            f"💨 Slip: <b>-{slippage_pct:.3f}%</b>\n"
            f"📉 Net: <b>{net_pct:+.3f}%</b>\n"
            f"💵 <b>Профит: +{net_profit_usd:.2f} USD</b>\n"
            f"⏱ age: {age_buy_ms}ms / {age_sell_ms}ms"
            f"{sizes_block}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Проверяйте статус вывода монеты перед исполнением</i>"
        )

        key = f"{symbol}|{buy_exchange}|{sell_exchange}"

        # <<< ИЗМЕНЕНО: для 🚀 при включённой опции игнорируем cooldown
        ignore_cooldown = (
            TELEGRAM_ROCKET_NO_COOLDOWN and emoji == "🚀"
        )
        return self.send(
            text,
            key=key,
            ignore_cooldown=ignore_cooldown,
        )

    def send_status(self, text: str) -> bool:
        return self.send(f"ℹ️ <b>Статус бота</b>\n{text}")

    # ------------------------------------------------------------------ #
    # <<< ДОБАВЛЕНО: диагностика для /stats                              #
    # ------------------------------------------------------------------ #
    def get_counters(self) -> dict:
        return {
            "sent": self._sent_count,
            "filtered": self._filtered_count,
            "cooldown_skipped": self._cooldown_skipped,
            "min_net_pct": TELEGRAM_MIN_NET_PCT,
            "rocket_net_pct": TELEGRAM_ROCKET_NET_PCT,
        }

    # ------------------------------------------------------------------ #
    # Внутренние                                                         #
    # ------------------------------------------------------------------ #
    def _is_in_cooldown(self, key: str) -> bool:
        with self._lock:
            last = self._last_sent.get(key)
        if last is None:
            return False
        return (time.time() - last) < self.cooldown_sec

    def _post_with_retries(
        self,
        text: str,
        parse_mode: str,
        disable_notification: bool,
        max_attempts: int = 3,
    ) -> bool:
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
            "disable_notification": disable_notification,
        }

        for attempt in range(1, max_attempts + 1):
            try:
                r = requests.post(url, json=payload, timeout=10)

                if r.status_code == 200 and r.json().get("ok"):
                    return True

                if r.status_code == 429:
                    try:
                        retry_after = r.json().get("parameters", {}).get("retry_after", 2)
                    except Exception:
                        retry_after = 2
                    time.sleep(retry_after)
                    continue

                if 400 <= r.status_code < 500:
                    logger.error(f"Telegram 4xx: {r.status_code} {r.text[:200]}")
                    return False

            except requests.exceptions.RequestException as e:
                logger.warning(f"Telegram network error (attempt {attempt}): {e}")

            time.sleep(2 ** (attempt - 1))

        logger.error("Telegram: все попытки исчерпаны")
        return False


notifier = TelegramNotifier()