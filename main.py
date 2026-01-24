from binance.client import Client
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
import threading
import requests
import os

# ================= НАСТРОЙКИ =================
MIN_24H_VOLUME = 70_000_000
LOOKBACK_CANDLES = 108

VOL_MULT_TREND = 2.0
VOL_MULT_COUNTER = 5.0

EMA_FAST = 20
EMA_SLOW = 200

MIN_BODY_TREND = 10.0
MIN_BODY_COUNTER = 0.0  # контртренд хвостатые свечи допускаются

ATR_LEN = 50
ATR_GAP_MULT = 0.8
EMA20_PROXIMITY_MULT = 0.5

COOLDOWN_BARS = 0
SLEEP = 300

CHAT_ID = os.getenv("CHAT_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")

client = Client()

# ================= TELEGRAM =================
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"Ошибка Telegram: {e}")

# ================= VWAP =================
def calculate_vwap(df):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    return (tp * df["volume"]).cumsum() / df["volume"].cumsum()

# ================= ATR =================
def calculate_atr(df, period=50):
    df['h-l'] = df['high'] - df['low']
    df['h-c'] = abs(df['high'] - df['close'].shift(1))
    df['l-c'] = abs(df['low'] - df['close'].shift(1))
    tr = df[['h-l','h-c','l-c']].max(axis=1)
    atr = tr.rolling(period).mean()
    return atr

# ================= LIQUID SYMBOLS =================
def get_liquid_futures_symbols():
    tickers = client._request_futures_api(method="get", path="ticker/24hr")
    return [
        t["symbol"]
        for t in tickers
        if t["symbol"].endswith("USDT")
        and float(t["quoteVolume"]) >= MIN_24H_VOLUME
    ]

# ================= SIGNAL CHECK =================
def check_volume_signal(symbol):
    klines = client.futures_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_5MINUTE,
        limit=LOOKBACK_CANDLES
    )

    df = pd.DataFrame(klines, columns=[
        "open_time","open","high","low","close",
        "volume","close_time","quote_volume",
        "trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)

    # ================= ОБЪЁМ =================
    df["quote_volume"] = df["close"] * df["volume"]
    avg_volume = df["quote_volume"][:-2].mean()
    last_volume = df["quote_volume"].iloc[-2]

    if avg_volume == 0 or last_volume == 0:
        return None

    volX = last_volume / avg_volume
    volText = f"x{volX:.2f}"

    volumeSpikeTrend = last_volume >= avg_volume * VOL_MULT_TREND
    volumeSpikeCounter = last_volume >= avg_volume * VOL_MULT_COUNTER

    # ================= EMA =================
    df["ema20"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    # ================= ATR =================
    df["atr"] = calculate_atr(df, ATR_LEN)
    atrValue = df["atr"].iloc[-2]

    # ================= VWAP =================
    df["vwap"] = calculate_vwap(df)
    last = df.iloc[-2]

    # ================= СВЕЧА =================
    body = abs(last.close - last.open)
    candle_range = last.high - last.low
    body_pct = 0 if candle_range == 0 else body / candle_range * 100

    bull = last.close > last.open
    bear = last.close < last.open

    # ================= УСЛОВИЯ =================
    strong_body_trend = body_pct >= MIN_BODY_TREND
    strong_body_counter = body_pct >= MIN_BODY_COUNTER

    # переменные для контртренда
    below_ema20 = last.low < last.ema20
    above_ema20 = last.high > last.ema20
    below_vwap = last.low < last.vwap
    above_vwap = last.high > last.vwap

    # трендовые сигналы проверяем низ/верх свечи против всех EMA/VWAP
    buy_low_condition = last.low < last.ema20 and last.low < last.ema200 and last.low < last.vwap
    sell_high_condition = last.high > last.ema20 and last.high > last.ema200 and last.high > last.vwap

    bull_trend = last.ema20 > last.ema200
    bear_trend = last.ema20 < last.ema200

    emas_far_enough = abs(last.ema20 - last.ema200) >= atrValue * ATR_GAP_MULT
    ema20_far_vwap = abs(last.ema20 - last.vwap) >= atrValue * EMA20_PROXIMITY_MULT
    ema20_far_ema200 = abs(last.ema20 - last.ema200) >= atrValue * EMA20_PROXIMITY_MULT
    ema20_clear_zone = ema20_far_vwap and ema20_far_ema200

    signals = []

    # ================= TREND =================
    if volumeSpikeTrend and bull and strong_body_trend and buy_low_condition and bull_trend and emas_far_enough and ema20_far_vwap:
        signals.append("BUY_TREND")
    if volumeSpikeTrend and bear and strong_body_trend and sell_high_condition and bear_trend and emas_far_enough and ema20_far_vwap:
        signals.append("SELL_TREND")

    # ================= COUNTER =================
    if volumeSpikeCounter and bull and strong_body_counter and below_ema20 and below_vwap and bear_trend and emas_far_enough and ema20_clear_zone:
        signals.append("BUY_COUNTER")
    if volumeSpikeCounter and bear and strong_body_counter and above_ema20 and above_vwap and bull_trend and emas_far_enough and ema20_clear_zone:
        signals.append("SELL_COUNTER")

    if not signals:
        return None

    return {
        "symbol": symbol,
        "signals": signals,
        "volume_ratio": volX,
        "volText": volText,
        "close": last.close,
        "low": last.low,
        "high": last.high,
        "ema20": last.ema20,
        "ema200": last.ema200,
        "vwap": last.vwap
    }

# ================= WAIT =================
def sleep_until_next_5m():
    now = datetime.now(timezone.utc)
    wait = 300 - ((now.minute * 60 + now.second) % 300)
    time.sleep(wait)

# ================= MAIN =================
def main():
    symbols = get_liquid_futures_symbols()
    last_update = time.time()
    print(f"🚀 Старт | Монет: {len(symbols)}")

    while True:
        if time.time() - last_update > 3600:
            symbols = get_liquid_futures_symbols()
            last_update = time.time()

        print("\n🔍 Проверка...")
        found = 0

        for s in symbols:
            try:
                res = check_volume_signal(s)
                if res:
                    beep_signal()
                    found += 1

                    # LOW для BUY_TREND, HIGH для SELL_TREND
                    extra_info = ""
                    if "BUY_TREND" in res['signals']:
                        extra_info = f"\nLOW: {res['low']:.6f}"
                    if "SELL_TREND" in res['signals']:
                        extra_info = f"\nHIGH: {res['high']:.6f}"

                    msg = (
                        f"🔥 {res['symbol']}\n"
                        f"Тип: {', '.join(res['signals'])}\n"
                        f"Close: {res['close']:.6f}\n"
                        f"EMA20: {res['ema20']:.6f}\n"
                        f"EMA200: {res['ema200']:.6f}\n"
                        f"VWAP: {res['vwap']:.6f}\n"
                        f"VOL {res['volText']}{extra_info}"
                    )

                    print(msg)
                    send_telegram(msg)
            except Exception as e:
                print(f"{s}: {e}")

        print(f"✅ Найдено сигналов: {found}")
        sleep_until_next_5m()

if __name__ == "__main__":
    main()
