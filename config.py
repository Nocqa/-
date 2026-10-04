# config.py
"""
Конфигурация арбитражного бота.

Источники значений:
- переменные окружения из .env (приоритет)
- значения по умолчанию (если .env не задан)

Секции:
1. Telegram
2. Символы для мониторинга (fallback)
3. Биржи
4. Пороги арбитража
5. Фильтры качества котировок (stale / skew)
6. Комиссии бирж и слиппедж
7. WebSocket-эндпоинты
7b. Gate WebSocket (OBU) — выбор канала и флаги
8. Логирование
9. Динамический список арбитражных пар
10. VWAP / executable liquidity
11. Bybit
12. Диагностика
"""
import os
from dotenv import load_dotenv

load_dotenv()


# --------------------------------------------------------------------- #
# 1. Telegram                                                           #
# --------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0") or "0")
TELEGRAM_COOLDOWN_SEC = int(os.getenv("TELEGRAM_COOLDOWN_SEC", "60") or "60")

# Минимальный net% для отправки сигнала в Telegram.
# Сигналы с net_pct < этого значения НЕ отправляются в Telegram,
# но пишутся в stats/signals.log и в лог бота.
# 0.5% → в Telegram идут только 💰 (0.5..1.5) и 🚀 (>=1.5)
TELEGRAM_MIN_NET_PCT = float(
    os.getenv("TELEGRAM_MIN_NET_PCT", "0.5") or "0.5"
)

# Порог для 🚀 (ракета).
TELEGRAM_ROCKET_NET_PCT = float(
    os.getenv("TELEGRAM_ROCKET_NET_PCT", "1.5") or "1.5"
)

# Если True — 🚀 игнорирует cooldown (чтобы не пропустить жирный сигнал).
TELEGRAM_ROCKET_NO_COOLDOWN = (
    os.getenv("TELEGRAM_ROCKET_NO_COOLDOWN", "true").lower() == "true"
)


# --------------------------------------------------------------------- #
# 2. Символы для мониторинга (FALLBACK)                                 #
# --------------------------------------------------------------------- #
# TON-USDT убран — Bybit не листит TON/USDT (Invalid symbol).
# MATIC-USDT убран — MEXC переименовал MATIC в POL (Invalid symbol).
SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT",
    "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT",
    "LTC-USDT", "TRX-USDT", "PEPE-USDT",
    "WIF-USDT", "SHIB-USDT", "SUI-USDT", "APT-USDT", "ARB-USDT",
    "OP-USDT", "INJ-USDT", "SEI-USDT", "TIA-USDT",
]


# --------------------------------------------------------------------- #
# 3. Активные биржи                                                     #
# --------------------------------------------------------------------- #
EXCHANGES = ["bybit", "gate", "mexc", "kucoin"]


# --------------------------------------------------------------------- #
# 4. Пороги арбитража                                                   #
# --------------------------------------------------------------------- #
MIN_SPREAD_PCT = float(os.getenv("MIN_SPREAD_PCT", "0.05") or "0.05")
MIN_PROFIT_USD = float(os.getenv("MIN_PROFIT_USD", "0.10") or "0.10")
TRADE_VOLUME_USD = float(os.getenv("TRADE_VOLUME_USD", "200.0") or "200.0")


# --------------------------------------------------------------------- #
# 5. Фильтры качества котировок                                         #
# --------------------------------------------------------------------- #
MAX_QUOTE_AGE_MS = int(os.getenv("MAX_QUOTE_AGE_MS", "250") or "250")
MAX_QUOTE_SKEW_MS = int(os.getenv("MAX_QUOTE_SKEW_MS", "150") or "150")

MAX_QUOTE_AGE_SEC = MAX_QUOTE_AGE_MS / 1000.0
MAX_QUOTE_SKEW_SEC = MAX_QUOTE_SKEW_MS / 1000.0

ARBITRAGE_INTERVAL_SEC = float(os.getenv("ARBITRAGE_INTERVAL_SEC", "0.5") or "0.5")
PRINTER_INTERVAL_SEC = float(os.getenv("PRINTER_INTERVAL_SEC", "5.0") or "5.0")


# --------------------------------------------------------------------- #
# 6. Комиссии бирж и слиппедж                                           #
# --------------------------------------------------------------------- #
FEES = {
    "bybit":   float(os.getenv("FEE_BYBIT",   "0.18") or "0.18"),
    "gate":    float(os.getenv("FEE_GATE",    "0.20") or "0.20"),
    "mexc":    float(os.getenv("FEE_MEXC",    "0.05") or "0.05"),
    "kucoin":  float(os.getenv("FEE_KUCOIN",  "0.10") or "0.10"),
    "binance": float(os.getenv("FEE_BINANCE", "0.10") or "0.10"),
}

SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.01") or "0.01")


# --------------------------------------------------------------------- #
# 7. WebSocket-эндпоинты                                                #
# --------------------------------------------------------------------- #
WS_URLS = {
    "bybit":  "wss://stream.bybit.com/v5/public/spot",
    "gate":   "wss://api.gateio.ws/ws/v4/",
    "mexc":   "wss://wbs-api.mexc.com/ws",
    "kucoin": "https://api.kucoin.com",
}


# --------------------------------------------------------------------- #
# 7b. Gate WebSocket — OBU (Order Book V2)                              #
# --------------------------------------------------------------------- #
# True  → использовать spot.obu (первое сообщение — full snapshot,
#         REST-синхронизация не нужна).
# False → использовать старый spot.order_book_update.
GATE_USE_OBU = os.getenv("GATE_USE_OBU", "true").lower() == "true"

# Уровень глубины OBU: 50 (20ms) или 400 (100ms)
GATE_OBU_LEVEL = os.getenv("GATE_OBU_LEVEL", "50").strip()

# Пауза между subscribe-запросами (по одной паре) — сек
GATE_SUBSCRIBE_PAUSE = float(
    os.getenv("GATE_SUBSCRIBE_PAUSE", "0.02") or "0.02"
)

# Подробное логирование RAW-сообщений Gate (только для отладки)
GATE_DEBUG_RAW = (
    os.getenv("GATE_DEBUG_RAW", "false").lower() == "true"
)


# --------------------------------------------------------------------- #
# 8. Логирование                                                        #
# --------------------------------------------------------------------- #
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# --------------------------------------------------------------------- #
# 9. Динамический список арбитражных пар                                #
# --------------------------------------------------------------------- #
PAIR_REFRESH_SECONDS = int(os.getenv("PAIR_REFRESH_SECONDS", "600") or "600")
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "500000") or "500000")
MAX_ACTIVE_PAIRS = int(os.getenv("MAX_ACTIVE_PAIRS", "80") or "80")

MAX_INTERNAL_SPREAD_PCT = float(
    os.getenv("MAX_INTERNAL_SPREAD_PCT", "0.30") or "0.30"
)

EXCLUDED_BASES = {
    "USDC", "USDE", "FDUSD", "DAI",
    "USD1", "TUSD", "BUSD", "USDD",
    "PYUSD", "USDP", "GUSD", "USTC",
}

PRINT_DISCOVERED_PAIRS = (
    os.getenv("PRINT_DISCOVERED_PAIRS", "true").lower() == "true"
)


# --------------------------------------------------------------------- #
# 10. VWAP / executable liquidity                                       #
# --------------------------------------------------------------------- #
ORDERBOOK_DEPTH = int(os.getenv("ORDERBOOK_DEPTH", "20") or "20")

MAX_EXECUTION_SLIPPAGE_PCT = float(
    os.getenv("MAX_EXECUTION_SLIPPAGE_PCT", "0.05") or "0.05"
)

_trade_sizes_raw = os.getenv("TRADE_SIZES_USD", "50,100,200") or "50,100,200"
TRADE_SIZES_USD = [
    float(x.strip()) for x in _trade_sizes_raw.split(",") if x.strip()
]

STRONG_SIGNAL_NET_PCT = float(
    os.getenv("STRONG_SIGNAL_NET_PCT", "0.5") or "0.5"
)

REST_SNAPSHOT_ENABLED = (
    os.getenv("REST_SNAPSHOT_ENABLED", "true").lower() == "true"
)


# --------------------------------------------------------------------- #
# 11. Bybit                                                              #
# --------------------------------------------------------------------- #
# Глубина стакана Bybit: 1 / 50 / 200 / 500
# 50 — оптимально (20 ms, snapshot + delta)
BYBIT_ORDERBOOK_DEPTH = int(os.getenv("BYBIT_ORDERBOOK_DEPTH", "50") or "50")

# Ping interval для WS (Bybit сам шлёт ping каждые 20 сек)
BYBIT_PING_INTERVAL = int(os.getenv("BYBIT_PING_INTERVAL", "20") or "20")

# Максимум топиков в одном subscribe/unsubscribe сообщении
BYBIT_SUBSCRIBE_CHUNK = int(os.getenv("BYBIT_SUBSCRIBE_CHUNK", "10") or "10")


# --------------------------------------------------------------------- #
# 12. Диагностика                                                       #
# --------------------------------------------------------------------- #
# Символы, по которым раз в DIAG_INTERVAL_SEC печатается подробный стейт.
_diag_syms_raw = os.getenv(
    "DIAG_SYMBOLS", "ZRO-USDT,BTC-USDT,FET-USDT"
) or "ZRO-USDT,BTC-USDT,FET-USDT"
DIAG_SYMBOLS = [s.strip() for s in _diag_syms_raw.split(",") if s.strip()]

# Интервал диагностики (сек). На время отладки можно уменьшить до 10.
DIAG_INTERVAL_SEC = int(os.getenv("DIAG_INTERVAL_SEC", "30") or "30")