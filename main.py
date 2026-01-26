from binance.client import Client
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# ================= НАСТРОЙКИ (1-в-1 с Pine) =================
MIN_24H_VOLUME = 70_000_000
LOOKBACK = 108

VOL_MULT_TREND = 2.0
VOL_MULT_COUNTER = 5.0

EMA_FAST = 20
EMA_SLOW = 200

MIN_BODY_PCT = 10.0
COOLDOWN_BARS = 0

ATR_LEN = 50
ATR_GAP_MULT = 0.8
EMA20_PROX_MULT = 0.5
EMA200_PROX_MULT = 1.0

SLEEP = 300

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

client = Client()

# ================= TELEGRAM =================
def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except:
        pass

# ================= ATR =================
def calculate_atr(df, length):
    high = df['high']
    low = df['low']
    close = df['close']

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    return tr.rolling(length).mean()

# ================= VWAP =================
def calculate_vwap(df):
    hlc3 = (df['high'] + df['low'] + df['close']) / 3
    return (hlc3 * df['volume']).cumsum() / df['volume'].cumsum()

# ================= LIQUID SYMBOLS =================
def get_symbols():
    tickers = client._request_futures_api("get", "ticker/24hr")
    return [
        t["symbol"]
        for t in tickers
        if t["symbol"].endswith("USDT")
        and float(t["quoteVolume"]) >= MIN_24H_VOLUME
    ]

# ================= SIGNAL LOGIC =================
def check_symbol(symbol):
    klines = client.futures_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_5MINUTE,
        limit=LOOKBACK
    )

    df = pd.DataFrame(klines, columns=[
        "open_time","open","high","low","close",
        "volume","close_time","qv","trades",
        "tb","tq","ignore"
    ])

    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)

    # ===== Индикаторы =====
    df["ema20"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["atr"] = calculate_atr(df, ATR_LEN)
    df["vwap"] = calculate_vwap(df)

    last = df.iloc[-2]  # ЗАКРЫТАЯ СВЕЧА

    # ===== Volume =====
    df["quote_vol"] = df["close"] * df["volume"]
    avg_vol = df["quote_vol"][:-2].mean()
    vol_x = last.quote_vol / avg_vol if avg_vol > 0 else 0

    spike_trend = last.quote_vol >= avg_vol * VOL_MULT_TREND
    spike_counter = last.quote_vol >= avg_vol * VOL_MULT_COUNTER

    # ===== Candle =====
    body = abs(last.close - last.open)
    rng = last.high - last.low
    body_pct = (body / rng * 100) if rng > 0 else 0

    bull = last.close > last.open
    bear = last.close < last.open

    strong_body = body_pct >= MIN_BODY_PCT

    # ===== Trend =====
    bull_trend = last.ema20 > last.ema200
    bear_trend = last.ema20 < last.ema200

    # ===== Filters =====
    emas_far = abs(last.ema20 - last.ema200) >= last.atr * ATR_GAP_MULT

    ema20_vwap_far = abs(last.ema20 - last.vwap) >= last.atr * EMA20_PROX_MULT
    ema200_vwap_far = abs(last.ema200 - last.vwap) >= last.atr * EMA200_PROX_MULT
    ema20_ema200_far = abs(last.ema20 - last.ema200) >= last.atr * EMA20_PROX_MULT

    ema20_clear_zone = (
        ema20_vwap_far and
        ema200_vwap_far and
        ema20_ema200_far
    )

    # ===== PRICE POSITION =====
    below_ema20 = last.open < last.ema20 and last.close < last.ema20
    above_ema20 = last.open > last.ema20 and last.close > last.ema20

    below_vwap = last.close < last.vwap
    above_vwap = last.close > last.vwap

    buy_low = last.low < last.ema20 and last.low < last.ema200
    sell_high = last.high > last.ema20 and last.high > last.ema200

    signals = []

    # ================= TREND =================
    if spike_trend and bull and strong_body and below_ema20 and below_vwap and bull_trend and emas_far and buy_low and ema20_vwap_far:
        signals.append("BUY_TREND")

    if spike_trend and bear and strong_body and above_ema20 and above_vwap and bear_trend and emas_far and sell_high and ema20_vwap_far:
        signals.append("SELL_TREND")

    # ================= COUNTER =================
    if spike_counter and bull and strong_body and below_ema20 and below_vwap and bear_trend and emas_far and ema20_clear_zone:
        signals.append("BUY_COUNTER")

    if spike_counter and bear and strong_body and above_ema20 and above_vwap and bull_trend and emas_far and ema20_clear_zone:
        signals.append("SELL_COUNTER")

    if not signals:
        return None

    return {
        "symbol": symbol,
        "signals": signals,
        "vol": f"x{vol_x:.2f}",
        "close": last.close
    }

# ================= MAIN LOOP =================
def main():
    symbols = get_symbols()
    print(f"🚀 Старт | {len(symbols)} символов")

    while True:
        for s in symbols:
            try:
                res = check_symbol(s)
                if res:
                    msg = (
                        f"🔥 {res['symbol']}\n"
                        f"{', '.join(res['signals'])}\n"
                        f"Close: {res['close']:.6f}\n"
                        f"VOL {res['vol']}"
                    )
                    print(msg)
                    send_telegram(msg)
            except Exception as e:
                print(s, e)

        time.sleep(SLEEP)

if __name__ == "__main__":
    main()
