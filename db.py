import sqlite3
import os
from threading import Lock
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "trades.db")
_DB_LOCK = Lock()


def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Создаёт таблицы если их нет. Вызвать один раз при старте бота."""
    with _DB_LOCK:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id          TEXT PRIMARY KEY,
                bot_name    TEXT,
                symbol      TEXT,
                side        TEXT,
                entry_price REAL,
                open_time   TEXT,
                close_time  TEXT,
                natr        REAL,
                vol_text    TEXT,
                vol_24h     REAL,
                corr_btc    TEXT,
                signals     TEXT,
                swing_num   INTEGER,
                delta_pct   REAL,
                relvol      TEXT,
                atr_ratio_50  REAL,
                atr_ratio_75  REAL,
                atr_ratio_100 REAL,
                expansion_3   REAL,
                expansion_5   REAL
            );

            CREATE TABLE IF NOT EXISTS trade_strategies (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id    TEXT REFERENCES trades(id),
                strategy    TEXT,
                tp          REAL,
                sl          REAL,
                status      TEXT DEFAULT 'OPEN',
                close_time  TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_symbol
                ON trades(symbol);
            CREATE INDEX IF NOT EXISTS idx_strategies_trade
                ON trade_strategies(trade_id);
            CREATE INDEX IF NOT EXISTS idx_strategies_status
                ON trade_strategies(status);
        """)
        conn.commit()
        conn.close()
    print(f"✅ БД инициализирована: {DB_FILE}")


# ================================================================
# ЗАПИСЬ новой сделки
# ================================================================

def insert_trade(trade_id, bot_name, trade_info, vol_text, vol_24h, corr_text):
    """
    Вызывать в том же месте где write_trade_to_excel.
    trade_info — тот же dict что передаётся в write_trade_to_excel.
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO trades
                    (id, bot_name, symbol, side, entry_price, open_time,
                     natr, vol_text, vol_24h, corr_btc, signals,
                     swing_num, delta_pct, relvol,
                     atr_ratio_50, atr_ratio_75, atr_ratio_100,
                     expansion_3, expansion_5)
                VALUES
                    (?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?,
                     ?, ?, ?,
                     ?, ?, ?,
                     ?, ?)
            """, (
                trade_id,
                bot_name,
                trade_info["symbol"],
                trade_info.get("side", ""),
                trade_info["entry_price"],
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                trade_info.get("natr"),
                vol_text,
                vol_24h,
                str(corr_text),
                ", ".join(trade_info.get("signals", [])),
                trade_info.get("swing_num"),
                trade_info.get("delta_pct"),
                str(trade_info.get("relvol", "N/A")),
                trade_info.get("atr_ratio_50"),
                trade_info.get("atr_ratio_75"),
                trade_info.get("atr_ratio_100"),
                trade_info.get("expansion_3"),
                trade_info.get("expansion_5"),
            ))

            # Пять строк для пяти стратегий
            for strat_name, strat_data in trade_info["strategies"].items():
                conn.execute("""
                    INSERT INTO trade_strategies
                        (trade_id, strategy, tp, sl, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    trade_id,
                    strat_name,
                    strat_data["tp"],
                    strat_data["sl"],
                    strat_data.get("status", "OPEN"),  # ← берём реальный статус
                ))

            conn.commit()
        except Exception as e:
            import traceback
            print(f"Ошибка insert_trade {trade_id}: {e}")
            traceback.print_exc()
        finally:
            conn.close()


# ================================================================
# ОБНОВЛЕНИЕ статуса стратегии (TP / SL)
# ================================================================

def update_strategy_status(trade_id, strategy_name, status):
    """
    Вызывать в том же месте где update_trade_status_in_excel.
    status: 'TP' или 'SL'
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                UPDATE trade_strategies
                SET status = ?, close_time = ?
                WHERE trade_id = ? AND strategy = ?
            """, (status, now, trade_id, strategy_name))

            # Если все стратегии закрыты — проставляем close_time на саму сделку
            row = conn.execute("""
                SELECT COUNT(*) as cnt
                FROM trade_strategies
                WHERE trade_id = ? AND status = 'OPEN'
            """, (trade_id,)).fetchone()

            if row["cnt"] == 0:
                conn.execute("""
                    UPDATE trades SET close_time = ? WHERE id = ?
                """, (now, trade_id))

            conn.commit()
        except Exception as e:
            import traceback
            print(f"Ошибка update_strategy_status {trade_id} {strategy_name}: {e}")
            traceback.print_exc()
        finally:
            conn.close()


# ================================================================
# ЧТЕНИЕ — для дашборда (FastAPI будет вызывать эти функции)
# ================================================================

def get_open_trades():
    """Все сделки у которых хотя бы одна стратегия ещё OPEN."""
    with _DB_LOCK:
        conn = get_conn()
        try:
            rows = conn.execute("""
                SELECT t.*,
                       s.strategy, s.tp, s.sl, s.status as strat_status, s.close_time as strat_close
                FROM trades t
                JOIN trade_strategies s ON s.trade_id = t.id
                WHERE t.id IN (
                    SELECT DISTINCT trade_id FROM trade_strategies WHERE status = 'OPEN'
                    AND tp IS NOT NULL
                )
                ORDER BY t.open_time DESC
            """).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_closed_trades(limit=3000, bot_name=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            if bot_name:
                rows = conn.execute("""
                    SELECT t.*,
                           s.strategy, s.tp, s.sl,
                           s.status as strat_status,
                           s.close_time as strat_close
                    FROM trades t
                    JOIN trade_strategies s ON s.trade_id = t.id
                    WHERE t.id IN (
                        SELECT id FROM trades
                        WHERE close_time IS NOT NULL AND bot_name = ?
                        ORDER BY close_time DESC
                        LIMIT ?
                    )
                    ORDER BY t.close_time DESC
                """, (bot_name, limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT t.*,
                           s.strategy, s.tp, s.sl,
                           s.status as strat_status,
                           s.close_time as strat_close
                    FROM trades t
                    JOIN trade_strategies s ON s.trade_id = t.id
                    WHERE t.id IN (
                        SELECT id FROM trades
                        WHERE close_time IS NOT NULL
                        ORDER BY close_time DESC
                        LIMIT ?
                    )
                    ORDER BY t.close_time DESC
                """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_stats(bot_name=None):
    """
    Статистика по стратегиям для дашборда.
    Возвращает dict: { '3:1': {'tp': N, 'sl': N, 'open': N, 'winrate': X}, ... }
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            if bot_name:
                rows = conn.execute("""
                    SELECT s.strategy, s.status, COUNT(*) as cnt
                    FROM trade_strategies s
                    JOIN trades t ON t.id = s.trade_id
                    WHERE t.bot_name = ?
                    GROUP BY s.strategy, s.status
                """, (bot_name,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT strategy, status, COUNT(*) as cnt
                    FROM trade_strategies
                    GROUP BY strategy, status
                """).fetchall()

            stats = {}
            for row in rows:
                strat = row["strategy"]
                if strat not in stats:
                    stats[strat] = {"tp": 0, "sl": 0, "open": 0, "winrate": 0.0}
                if row["status"] == "TP":
                    stats[strat]["tp"] = row["cnt"]
                elif row["status"] == "SL":
                    stats[strat]["sl"] = row["cnt"]
                elif row["status"] == "OPEN":
                    stats[strat]["open"] = row["cnt"]

            for strat, s in stats.items():
                total = s["tp"] + s["sl"]
                s["winrate"] = round(s["tp"] / total * 100, 1) if total > 0 else 0.0
                s["total"] = total

            return stats
        finally:
            conn.close()


def get_equity_curve(bot_name=None):
    """
    Возвращает список точек для equity curve.
    Каждая закрытая стратегия = +1 (TP) или -1 (SL).
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            if bot_name:
                rows = conn.execute("""
                    SELECT s.close_time, s.status, s.strategy
                    FROM trade_strategies s
                    JOIN trades t ON t.id = s.trade_id
                    WHERE s.status IN ('TP', 'SL') AND t.bot_name = ?
                    ORDER BY s.close_time ASC
                """, (bot_name,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT close_time, status, strategy
                    FROM trade_strategies
                    WHERE status IN ('TP', 'SL')
                    ORDER BY close_time ASC
                """).fetchall()

            equity = 0
            curve = []
            for row in rows:
                equity += 1 if row["status"] == "TP" else -1
                curve.append({
                    "time": row["close_time"],
                    "equity": equity,
                    "strategy": row["strategy"],
                    "result": row["status"],
                })
            return curve
        finally:
            conn.close()
    
def get_stats_detailed(bot_name=None, date_from=None, date_to=None):
    """
    Расширенная статистика по стратегиям:
    среднее время в сделке + средний фандинг.
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')"]
            params = []
            if bot_name:
                conditions.append("t.bot_name = ?")
                params.append(bot_name)
            if date_from:
                conditions.append("DATE(s.close_time) >= ?")
                params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?")
                params.append(date_to)
            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT s.strategy, s.status, s.close_time, t.open_time, t.symbol
                FROM trade_strategies s
                JOIN trades t ON t.id = s.trade_id
                WHERE {where}
            """, params).fetchall()

            from collections import defaultdict
            data = defaultdict(lambda: {
                "tp": 0, "sl": 0, "open": 0,
                "durations": [],
                "fundings": [],
            })

            FUNDING_RATE = 0.0001
            FUNDING_INTERVAL = 8 * 3600

            for row in rows:
                s = row["strategy"]
                if row["status"] == "TP":
                    data[s]["tp"] += 1
                else:
                    data[s]["sl"] += 1
                try:
                    open_dt  = datetime.strptime(row["open_time"],  "%Y-%m-%d %H:%M:%S")
                    close_dt = datetime.strptime(row["close_time"], "%Y-%m-%d %H:%M:%S")
                    duration = (close_dt - open_dt).total_seconds()
                    data[s]["durations"].append(duration)
                    funding_sessions = duration / FUNDING_INTERVAL
                    data[s]["fundings"].append(funding_sessions * FUNDING_RATE * 100)
                except Exception:
                    pass

            result = {}
            for s in data:
                d = data[s]
                total = d["tp"] + d["sl"]
                avg_dur = sum(d["durations"]) / len(d["durations"]) if d["durations"] else 0
                avg_fund = sum(d["fundings"]) / len(d["fundings"]) if d["fundings"] else 0
                hours   = int(avg_dur // 3600)
                minutes = int((avg_dur % 3600) // 60)
                result[s] = {
                    "tp":       d["tp"],
                    "sl":       d["sl"],
                    "open":     d["open"],
                    "total":    total,
                    "winrate":  round(d["tp"] / total * 100, 1) if total > 0 else 0,
                    "avg_duration": f"{hours}ч {minutes}м" if hours > 0 else f"{minutes}м",
                    "avg_duration_seconds": round(avg_dur),
                    "avg_funding_pct": round(avg_fund, 4),
                }
            return result
        finally:
            conn.close()

def get_daily_stats(bot_name=None, date_from=None, date_to=None, strategies=None, side=None):
    """
    Статистика по дням для столбчатой диаграммы.
    Возвращает список: [{date, tp, sl, pnl, winrate}, ...]
    """
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []

            if bot_name and bot_name != "all":
                conditions.append("t.bot_name = ?")
                params.append(bot_name)
            if date_from:
                conditions.append("DATE(s.close_time) >= ?")
                params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?")
                params.append(date_to)
            if strategies:
                placeholders = ",".join("?" * len(strategies))
                conditions.append(f"s.strategy IN ({placeholders})")
                params.extend(strategies)

            if side and side != "all":
                conditions.append("t.side = ?")
                params.append(side)

            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT
                    DATE(s.close_time) as date,
                    s.strategy,
                    s.status,
                    COUNT(*) as cnt
                FROM trade_strategies s
                JOIN trades t ON t.id = s.trade_id
                WHERE {where}
                GROUP BY DATE(s.close_time), s.strategy, s.status
                ORDER BY date ASC
            """, params).fetchall()

            STRAT_PCT = {
                "3:1": {"tp": 3, "sl": 1},
                "6:1": {"tp": 6, "sl": 1},
                "6:2": {"tp": 6, "sl": 2},
                "10:3": {"tp": 10, "sl": 3},
                "12:4": {"tp": 12, "sl": 4},
            }

            from collections import defaultdict
            days = defaultdict(lambda: {"tp": 0, "sl": 0, "pnl": 0.0})

            for row in rows:
                d = row["date"]
                s = row["strategy"]
                pct = STRAT_PCT.get(s, {"tp": 0, "sl": 0})
                if row["status"] == "TP":
                    days[d]["tp"] += row["cnt"]
                    days[d]["pnl"] += row["cnt"] * pct["tp"]
                else:
                    days[d]["sl"] += row["cnt"]
                    days[d]["pnl"] -= row["cnt"] * pct["sl"]

            result = []
            for date in sorted(days.keys()):
                d = days[date]
                total = d["tp"] + d["sl"]
                result.append({
                    "date":    date,
                    "tp":      d["tp"],
                    "sl":      d["sl"],
                    "pnl":     round(d["pnl"], 2),
                    "winrate": round(d["tp"] / total * 100, 1) if total > 0 else 0,
                })
            return result
        finally:
            conn.close()

def get_symbol_stats(bot_name=None, date_from=None, date_to=None, strategies=None, side=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []
            if bot_name and bot_name != "all":
                conditions.append("t.bot_name = ?"); params.append(bot_name)
            if date_from:
                conditions.append("DATE(s.close_time) >= ?"); params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?"); params.append(date_to)
            if strategies:
                conditions.append(f"s.strategy IN ({','.join('?'*len(strategies))})"); params.extend(strategies)
            if side and side != "all":
                conditions.append("t.side = ?")
                params.append(side)
            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT t.symbol, s.strategy, s.status, COUNT(*) as cnt
                FROM trade_strategies s JOIN trades t ON t.id=s.trade_id
                WHERE {where}
                GROUP BY t.symbol, s.strategy, s.status
            """, params).fetchall()
            STRAT_PCT = {"3:1":{"tp":3,"sl":1},"6:1":{"tp":6,"sl":1},"6:2":{"tp":6,"sl":2},"10:3":{"tp":10,"sl":3},"12:4":{"tp":12,"sl":4}}
            from collections import defaultdict
            syms = defaultdict(lambda: {"tp":0,"sl":0,"pnl":0.0})
            for row in rows:
                sym, strat, status, cnt = row["symbol"], row["strategy"], row["status"], row["cnt"]
                pct = STRAT_PCT.get(strat, {"tp":0,"sl":0})
                if status == "TP":
                    syms[sym]["tp"] += cnt; syms[sym]["pnl"] += cnt * pct["tp"]
                else:
                    syms[sym]["sl"] += cnt; syms[sym]["pnl"] -= cnt * pct["sl"]
            result = []
            for sym, d in syms.items():
                total = d["tp"] + d["sl"]
                result.append({"symbol":sym,"tp":d["tp"],"sl":d["sl"],
                    "pnl":round(d["pnl"],2),
                    "winrate":round(d["tp"]/total*100,1) if total>0 else 0})
            return sorted(result, key=lambda x: x["pnl"], reverse=True)
        finally:
            conn.close()


def get_weekday_stats(bot_name=None, date_from=None, date_to=None, strategies=None, side=None):
    with _DB_LOCK:
        conn = get_conn()
        try:
            conditions = ["s.status IN ('TP','SL')", "s.close_time IS NOT NULL"]
            params = []
            if bot_name and bot_name != "all":
                conditions.append("t.bot_name = ?"); params.append(bot_name)
            if date_from:
                conditions.append("DATE(s.close_time) >= ?"); params.append(date_from)
            if date_to:
                conditions.append("DATE(s.close_time) <= ?"); params.append(date_to)
            if strategies:
                conditions.append(f"s.strategy IN ({','.join('?'*len(strategies))})"); params.extend(strategies)
            if side and side != "all":
                conditions.append("t.side = ?")
                params.append(side)
            where = " AND ".join(conditions)
            rows = conn.execute(f"""
                SELECT strftime('%w', s.close_time) as dow,
                       s.strategy, s.status, COUNT(*) as cnt
                FROM trade_strategies s JOIN trades t ON t.id=s.trade_id
                WHERE {where}
                GROUP BY dow, s.strategy, s.status
            """, params).fetchall()
            STRAT_PCT = {"3:1":{"tp":3,"sl":1},"6:1":{"tp":6,"sl":1},"6:2":{"tp":6,"sl":2},"10:3":{"tp":10,"sl":3},"12:4":{"tp":12,"sl":4}}
            DAY_MAP = {"0":"Sunday","1":"Monday","2":"Tuesday","3":"Wednesday","4":"Thursday","5":"Friday","6":"Saturday"}
            ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            from collections import defaultdict
            days = defaultdict(lambda: {"tp":0,"sl":0,"pnl":0.0})
            for row in rows:
                day = DAY_MAP.get(row["dow"], row["dow"])
                pct = STRAT_PCT.get(row["strategy"], {"tp":0,"sl":0})
                if row["status"] == "TP":
                    days[day]["tp"] += row["cnt"]; days[day]["pnl"] += row["cnt"]*pct["tp"]
                else:
                    days[day]["sl"] += row["cnt"]; days[day]["pnl"] -= row["cnt"]*pct["sl"]
            result = []
            for day in ORDER:
                if day in days:
                    d = days[day]; total = d["tp"]+d["sl"]
                    result.append({"day":day,"tp":d["tp"],"sl":d["sl"],
                        "pnl":round(d["pnl"],2),
                        "winrate":round(d["tp"]/total*100,1) if total>0 else 0})
            return result
        finally:
            conn.close()

def manual_close_strategy(trade_id, strategy_name, close_price):
    with _DB_LOCK:
        conn = get_conn()
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            # Получаем данные стратегии
            row = conn.execute("""
                SELECT s.tp, s.sl, t.entry_price, t.side
                FROM trade_strategies s
                JOIN trades t ON t.id = s.trade_id
                WHERE s.trade_id = ? AND s.strategy = ?
            """, (trade_id, strategy_name)).fetchone()
            if not row:
                return {"error": "not found"}

            # Считаем PnL% чтобы определить TP или SL
            entry = row["entry_price"]
            side  = row["side"]
            if side == "BUY":
                pnl_pct = (close_price - entry) / entry * 100
            else:
                pnl_pct = (entry - close_price) / entry * 100

            # Если в плюсе — считаем как TP, в минусе — как SL
            status = "TP" if pnl_pct >= 0 else "SL"

            conn.execute("""
                UPDATE trade_strategies
                SET status = ?, close_time = ?
                WHERE trade_id = ? AND strategy = ?
            """, (status, now, trade_id, strategy_name))

            # Если все стратегии закрыты — закрываем сделку
            open_count = conn.execute("""
                SELECT COUNT(*) FROM trade_strategies
                WHERE trade_id = ? AND status = 'OPEN'
            """, (trade_id,)).fetchone()[0]
            if open_count == 0:
                conn.execute("UPDATE trades SET close_time = ? WHERE id = ?", (now, trade_id))

            conn.commit()
            return {"status": status, "pnl_pct": round(pnl_pct, 2)}
        except Exception as e:
            import traceback; traceback.print_exc()
            return {"error": str(e)}
        finally:
            conn.close()