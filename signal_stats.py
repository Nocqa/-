# core/signal_stats.py
"""
Статистика арбитражных сигналов.

Хранит сигналы в памяти (deque с ограничением) + пишет в stats/signals.log.
Раз в STATS_REFRESH_SECONDS пересчитывает сводки:
- stats/daily.txt   — за текущий день (UTC)
- stats/weekly.txt  — за последние 7 дней
- stats/monthly.txt — за последние 30 дней

Формат:
    signals.log      — построчный лог всех сигналов
    daily.txt        — топ-10 пар за день + по часам + по направлениям
    weekly.txt       — то же за неделю
    monthly.txt      — то же за месяц

ПАТЧ #6: _signals — deque(maxlen=MAX_SIGNALS_IN_MEMORY).
Память не растёт бесконечно, старые сигналы вытесняются автоматически.
"""
import threading
import time as _time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict

from loguru import logger


STATS_DIR = Path("stats")
SIGNALS_LOG = STATS_DIR / "signals.log"
DAILY_FILE = STATS_DIR / "daily.txt"
WEEKLY_FILE = STATS_DIR / "weekly.txt"
MONTHLY_FILE = STATS_DIR / "monthly.txt"

SIGNALS_LOG_HEADER = (
    "# ts_iso | symbol | buy_ex | sell_ex | buy_price | sell_price | "
    "gross_pct | fee_pct | slip_pct | net_pct | profit_usd | "
    "age_buy_ms | age_sell_ms | skew_ms\n"
)

# <<< ПАТЧ #6: максимум сигналов в памяти
MAX_SIGNALS_IN_MEMORY = 200_000

# --------------------------------------------------------------------- #
# Дедупликация                                                          #
# --------------------------------------------------------------------- #
SIGNAL_DEDUP_SEC = 1.0

# Статистика дедупликации
_last_signal_ts: Dict[str, float] = {}
_dedup_skipped: int = 0
_dedup_written: int = 0


# --------------------------------------------------------------------- #
# Внутреннее состояние                                                  #
# --------------------------------------------------------------------- #
_lock = threading.Lock()
_initialized = False
# <<< ПАТЧ #6: deque с ограничением
_signals: deque = deque(maxlen=MAX_SIGNALS_IN_MEMORY)


# --------------------------------------------------------------------- #
# Инициализация                                                         #
# --------------------------------------------------------------------- #
def _ensure_init() -> None:
    """Создать stats/ и signals.log с заголовком, если их ещё нет."""
    global _initialized
    if _initialized:
        return
    with _lock:
        if _initialized:
            return
        STATS_DIR.mkdir(exist_ok=True)
        if not SIGNALS_LOG.exists():
            with open(SIGNALS_LOG, "w", encoding="utf-8") as f:
                f.write(SIGNALS_LOG_HEADER)
        else:
            _load_existing()
        _initialized = True


def _load_existing() -> None:
    """
    Загрузить сигналы из signals.log за последние 30 дней.

    ПАТЧ #6: читаем построчно, но в памяти держим только
    последние MAX_SIGNALS_IN_MEMORY записей.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    loaded = 0
    skipped_old = 0
    skipped_bad = 0

    # Временный буфер — держит только последние MAX_SIGNALS_IN_MEMORY
    buffer: deque = deque(maxlen=MAX_SIGNALS_IN_MEMORY)

    try:
        with open(SIGNALS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 14:
                    skipped_bad += 1
                    continue

                try:
                    ts = datetime.strptime(
                        parts[0], "%Y-%m-%dT%H:%M:%S.%fZ"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    skipped_bad += 1
                    continue

                if ts < cutoff:
                    skipped_old += 1
                    continue

                try:
                    buffer.append({
                        "ts": parts[0],
                        "symbol": parts[1],
                        "buy_ex": parts[2],
                        "sell_ex": parts[3],
                        "buy_price": float(parts[4]),
                        "sell_price": float(parts[5]),
                        "gross_pct": float(parts[6]),
                        "fee_pct": float(parts[7]),
                        "slip_pct": float(parts[8]),
                        "net_pct": float(parts[9]),
                        "profit_usd": float(parts[10]),
                        "age_buy_ms": int(parts[11]),
                        "age_sell_ms": int(parts[12]),
                        "skew_ms": int(parts[13]),
                    })
                    loaded += 1
                except (ValueError, IndexError):
                    skipped_bad += 1
                    continue

        # Переносим в основной deque (тоже с ограничением)
        _signals.extend(buffer)

        logger.info(
            f"signal_stats: загружено {loaded} сигналов "
            f"(в памяти: {len(_signals)}/{MAX_SIGNALS_IN_MEMORY}, "
            f"пропущено: {skipped_old} старше 30 дней, {skipped_bad} битых)"
        )
    except Exception as e:
        logger.error(f"signal_stats load error: {e}")


# --------------------------------------------------------------------- #
# Запись сигнала                                                        #
# --------------------------------------------------------------------- #
def log_signal(op: dict) -> None:
    """
    Записать сигнал в память и в signals.log.

    С дедупликацией: один и тот же (symbol, buy_ex, sell_ex)
    пишется не чаще, чем раз в SIGNAL_DEDUP_SEC секунд.
    """
    global _dedup_skipped, _dedup_written
    _ensure_init()

    # --- ДЕДУПЛИКАЦИЯ ---
    key = f"{op.get('symbol')}|{op.get('buy_exchange')}|{op.get('sell_exchange')}"
    now = _time.time()
    last = _last_signal_ts.get(key, 0.0)
    if now - last < SIGNAL_DEDUP_SEC:
        _dedup_skipped += 1
        return
    _last_signal_ts[key] = now
    _dedup_written += 1
    # --- КОНЕЦ ДЕДУПЛИКАЦИИ ---

    ts_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    entry = {
        "ts": ts_iso,
        "symbol": op.get("symbol", "?"),
        "buy_ex": op.get("buy_exchange", "?"),
        "sell_ex": op.get("sell_exchange", "?"),
        "buy_price": float(op.get("buy_price", 0)),
        "sell_price": float(op.get("sell_price", 0)),
        "gross_pct": float(op.get("spread_pct", 0)),
        "fee_pct": float(op.get("fee_pct", 0)),
        "slip_pct": float(op.get("slippage_pct", 0)),
        "net_pct": float(op.get("net_spread_pct", 0)),
        "profit_usd": float(op.get("net_profit_usd", 0)),
        "age_buy_ms": int(op.get("buy_age_ms", 0)),
        "age_sell_ms": int(op.get("sell_age_ms", 0)),
        "skew_ms": int(op.get("skew_ms", 0)),
    }

    with _lock:
        _signals.append(entry)
        try:
            line = (
                f"{entry['ts']} | {entry['symbol']} | {entry['buy_ex']} | "
                f"{entry['sell_ex']} | {entry['buy_price']:.10g} | "
                f"{entry['sell_price']:.10g} | {entry['gross_pct']:.6f} | "
                f"{entry['fee_pct']:.6f} | {entry['slip_pct']:.6f} | "
                f"{entry['net_pct']:.6f} | {entry['profit_usd']:.4f} | "
                f"{entry['age_buy_ms']} | {entry['age_sell_ms']} | "
                f"{entry['skew_ms']}\n"
            )
            with open(SIGNALS_LOG, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logger.error(f"signal_stats write error: {e}")


def get_dedup_stats() -> dict:
    """Диагностика дедупликации (для Telegram /stats)."""
    return {
        "skipped": _dedup_skipped,
        "written": _dedup_written,
        "tracked_keys": len(_last_signal_ts),
        "dedup_sec": SIGNAL_DEDUP_SEC,
    }


# --------------------------------------------------------------------- #
# Фильтры и сводки                                                      #
# --------------------------------------------------------------------- #
def _filter_signals(days: int) -> List[dict]:
    """Сигналы за последние N дней."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = []
    for s in _signals:
        try:
            ts = datetime.strptime(s["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue
        if ts >= cutoff:
            result.append(s)
    return result


def _build_summary(title: str, signals: List[dict]) -> str:
    """Построить текстовую сводку по списку сигналов."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if not signals:
        return (
            f"{title}\n"
            f"Обновлено: {now}\n"
            f"\n"
            f"Сигналов за период: 0\n"
        )

    total = len(signals)
    total_profit = sum(s["profit_usd"] for s in signals)
    avg_net = sum(s["net_pct"] for s in signals) / total
    avg_skew = sum(s["skew_ms"] for s in signals) / total

    by_pair = defaultdict(lambda: {"count": 0, "profit": 0.0, "net_sum": 0.0})
    by_hour = defaultdict(int)
    by_dir = defaultdict(lambda: {"count": 0, "profit": 0.0})

    for s in signals:
        p = by_pair[s["symbol"]]
        p["count"] += 1
        p["profit"] += s["profit_usd"]
        p["net_sum"] += s["net_pct"]

        try:
            ts = datetime.strptime(s["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
            by_hour[ts.hour] += 1
        except ValueError:
            pass

        d = by_dir[f"{s['buy_ex'].upper()} → {s['sell_ex'].upper()}"]
        d["count"] += 1
        d["profit"] += s["profit_usd"]

    top_pairs = sorted(
        by_pair.items(),
        key=lambda kv: kv[1]["profit"],
        reverse=True,
    )[:10]

    lines = []
    lines.append(title)
    lines.append(f"Обновлено: {now}")
    lines.append("")
    lines.append(f"Всего сигналов: {total}")
    lines.append(f"Уникальных пар: {len(by_pair)}")
    lines.append(f"Общий профит: +{total_profit:.2f} USD")
    lines.append(f"Средний net: +{avg_net:.4f}%")
    lines.append(f"Средний skew: {avg_skew:.0f}ms")
    lines.append("")

    if top_pairs:
        best_name, best_data = top_pairs[0]
        best_avg_net = best_data["net_sum"] / best_data["count"]
        lines.append("--- ЛУЧШАЯ ПАРА ---")
        lines.append(
            f"{best_name} | {best_data['count']} сигналов | "
            f"профит +{best_data['profit']:.2f} USD | "
            f"avg net +{best_avg_net:.4f}%"
        )
        lines.append("")

    lines.append("--- ТОП-10 ПАР ---")
    for i, (name, data) in enumerate(top_pairs, 1):
        avg_net_pair = data["net_sum"] / data["count"]
        lines.append(
            f"{i:2d}. {name:12} | {data['count']:3d} сигналов | "
            f"профит +{data['profit']:6.2f} USD | "
            f"avg net +{avg_net_pair:.4f}%"
        )
    lines.append("")

    lines.append("--- ПО ЧАСАМ (UTC) ---")
    for h in range(24):
        cnt = by_hour.get(h, 0)
        bar = "█" * min(cnt, 50)
        lines.append(f"{h:02d}:00  {cnt:4d}  {bar}")
    lines.append("")

    lines.append("--- ПО НАПРАВЛЕНИЯМ ---")
    for direction, data in sorted(
        by_dir.items(), key=lambda kv: kv[1]["profit"], reverse=True
    ):
        lines.append(
            f"{direction:20} | {data['count']:3d} сигналов | "
            f"профит +{data['profit']:.2f} USD"
        )
    lines.append("")

    return "\n".join(lines)


def _write_file(path: Path, content: str) -> None:
    try:
        path.write_text(content, encoding="utf-8")
    except Exception as e:
        logger.error(f"signal_stats write {path}: {e}")


def refresh_all_stats() -> None:
    """Пересчитать все сводки и записать в файлы."""
    _ensure_init()

    with _lock:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        signals_today = [s for s in _signals if s["ts"].startswith(today)]
        signals_week = _filter_signals(7)
        signals_month = _filter_signals(30)
        total_signals = len(_signals)

    daily_title = f"=== СТАТИСТИКА ЗА ДЕНЬ {today} ==="
    _write_file(DAILY_FILE, _build_summary(daily_title, signals_today))

    week_start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    week_title = f"=== СТАТИСТИКА ЗА НЕДЕЛЮ ({week_start} — {today}) ==="
    _write_file(WEEKLY_FILE, _build_summary(week_title, signals_week))

    month_start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    month_title = f"=== СТАТИСТИКА ЗА МЕСЯЦ ({month_start} — {today}) ==="
    _write_file(MONTHLY_FILE, _build_summary(month_title, signals_month))

    logger.debug(
        f"signal_stats refreshed: in_memory={total_signals} "
        f"day={len(signals_today)} week={len(signals_week)} "
        f"month={len(signals_month)} | "
        f"dedup: skipped={_dedup_skipped} written={_dedup_written}"
    )