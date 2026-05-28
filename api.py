"""
Trading Dashboard API
Запуск: uvicorn api:app --host 0.0.0.0 --port 8765 --reload
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from datetime import datetime, timezone
from db import get_open_trades, get_closed_trades, get_stats, get_equity_curve, get_stats_detailed, get_daily_stats, get_symbol_stats, get_weekday_stats, manual_close_strategy

app = FastAPI(title="Trading Bot Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================================================
# Утилиты
# ================================================================

def group_trades_by_id(rows):
    """
    JOIN-запрос возвращает N строк на сделку (по числу стратегий).
    Эта функция схлопывает их в один объект:
    { trade_id: { ...trade_fields, strategies: { '3:1': {...}, ... } } }
    """
    trades = {}
    for row in rows:
        tid = row["id"]
        if tid not in trades:
            trades[tid] = {
                "id":          row["id"],
                "bot_name":    row["bot_name"],
                "symbol":      row["symbol"],
                "side":        row["side"],
                "entry_price": row["entry_price"],
                "open_time":   row["open_time"],
                "close_time":  row.get("close_time"),
                "natr":        row["natr"],
                "vol_text":    row["vol_text"],
                "vol_24h":     row["vol_24h"],
                "corr_btc":    row["corr_btc"],
                "signals":     row["signals"],
                "swing_num":   row["swing_num"],
                "delta_pct":   row["delta_pct"],
                "relvol":      row["relvol"],
                "strategies":  {},
            }
        trades[tid]["strategies"][row["strategy"]] = {
            "tp":         row["tp"],
            "sl":         row["sl"],
            "status":     row["strat_status"],
            "close_time": row.get("strat_close"),
        }
    return list(trades.values())


def enrich_open_trade(trade):
    """Добавляет поля которые нужны только для открытых позиций."""
    try:
        open_dt = datetime.strptime(trade["open_time"], "%Y-%m-%d %H:%M:%S")
        open_dt = open_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - open_dt
        hours   = int(delta.total_seconds() // 3600)
        minutes = int((delta.total_seconds() % 3600) // 60)
        trade["duration"] = f"{hours}ч {minutes}м"
        trade["duration_seconds"] = int(delta.total_seconds())
    except Exception:
        trade["duration"] = "—"
        trade["duration_seconds"] = 0
    return trade


# ================================================================
# REST эндпоинты
# ================================================================

@app.get("/trades/open")
def open_trades():
    rows   = get_open_trades()
    trades = group_trades_by_id(rows)
    trades = [enrich_open_trade(t) for t in trades]
    return {"trades": trades, "count": len(trades)}


@app.get("/trades/closed")
def closed_trades(limit: int = 3000, bot: str = None):
    rows   = get_closed_trades(limit=limit, bot_name=bot)
    trades = group_trades_by_id(rows)
    return {"trades": trades, "count": len(trades)}


@app.get("/stats")
def stats(bot: str = None):
    return get_stats(bot_name=bot)

@app.get("/stats/detailed")
def stats_detailed(bot: str = None, date_from: str = None, date_to: str = None):
    return get_stats_detailed(bot_name=bot, date_from=date_from, date_to=date_to)

@app.get("/stats/daily")
def daily_stats(
    bot: str = None,
    date_from: str = None,
    date_to: str = None,
    strategies: str = None  # через запятую: "3:1,6:1"
):
    strats = strategies.split(",") if strategies else None
    return get_daily_stats(
        bot_name=bot,
        date_from=date_from,
        date_to=date_to,
        strategies=strats
    )

@app.get("/stats/symbols")
def symbol_stats(bot: str = None, date_from: str = None, date_to: str = None, strategies: str = None):
    strats = strategies.split(",") if strategies else None
    return get_symbol_stats(bot_name=bot, date_from=date_from, date_to=date_to, strategies=strats)

@app.get("/stats/weekdays")
def weekday_stats(bot: str = None, date_from: str = None, date_to: str = None, strategies: str = None):
    strats = strategies.split(",") if strategies else None
    return get_weekday_stats(bot_name=bot, date_from=date_from, date_to=date_to, strategies=strats)

@app.post("/trades/{trade_id}/close/{strategy}")
def close_strategy(trade_id: str, strategy: str, price: float = 0):
    result = manual_close_strategy(trade_id, strategy, price)
    return result

@app.get("/equity")
def equity(bot: str = None):
    return get_equity_curve(bot_name=bot)


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ================================================================
# WebSocket — live обновления для дашборда
# ================================================================

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        msg = json.dumps(data)
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Сразу отправляем текущее состояние при подключении
        await websocket.send_text(json.dumps({
            "type":        "init",
            "open":        group_trades_by_id(get_open_trades()),
            "stats":       get_stats(),
            "stats_detail": get_stats_detailed(),
            "equity":      get_equity_curve(),
        }))

        # Держим соединение живым, слушаем пинги от клиента
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                # Шлём heartbeat каждые 30 секунд
                await websocket.send_text(json.dumps({"type": "heartbeat"}))

    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ================================================================
# Фоновая задача — пушим обновления клиентам каждые 5 секунд
# ================================================================

@app.on_event("startup")
async def start_background_push():
    asyncio.create_task(push_updates())


async def push_updates():
    """Каждые 5 секунд рассылает актуальные открытые позиции всем клиентам."""
    while True:
        await asyncio.sleep(5)
        if not manager.active:
            continue
        try:
            rows   = get_open_trades()
            trades = group_trades_by_id(rows)
            trades = [enrich_open_trade(t) for t in trades]
            await manager.broadcast({
                "type":        "update",
                "open":        trades,
                "stats":       get_stats(),
                "stats_detail": get_stats_detailed(),
                "equity":      get_equity_curve(),
            })
        except Exception as e:
            print(f"Ошибка push_updates: {e}")