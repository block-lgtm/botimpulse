from binance.client import Client
import pandas as pd
import time
from datetime import datetime, timezone
import requests
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ================= EXCEL =================
EXCEL_PATH = r"C:\Users\NVK\Desktop\CheckIndicator.xlsx"

try:
    df_excel = pd.read_excel(EXCEL_PATH, engine="openpyxl")
except FileNotFoundError:
    # Если файла нет — создаём пустой DataFrame с нужными колонками
    df_excel = pd.DataFrame(columns=[
        "Дата", "Время", "Тикет", "Сигнал", "Импульс"
    ])


# ================= НАСТРОЙКИ =================
MIN_24H_VOLUME = 70_000_000
LOOKBACK_CANDLES = 1500
VOLUME_LOOKBACK = 108

VOL_MULT_TREND = 2.0
VOL_MULT_COUNTER = 5.0

EMA_FAST = 20
EMA_SLOW = 200

MIN_BODY_TREND = 10.0
MIN_BODY_COUNTER = 10.0  # FIX: как в TV

ATR_LEN = 50
ATR_GAP_MULT = 0.8
EMA20_PROXIMITY_MULT = 0.5
EMA200_PROXIMITY_MULT = 1.0

COOLDOWN_BARS = 0
SLEEP = 300

CHAT_ID = os.getenv("CHAT_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")

client = Client()

BLACKLIST = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "PEPEUSDT",
    "SUIUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
}

# ================= TELEGRAM =================
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"Ошибка Telegram: {e}")

# ================= SESSION VWAP =================
def calculate_session_vwap(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.date
    tp = (df["high"] + df["low"] + df["close"]) / 3
    df["tpv"] = tp * df["volume"]
    df["cum_tpv"] = df.groupby("date")["tpv"].cumsum()
    df["cum_vol"] = df.groupby("date")["volume"].cumsum()
    return df["cum_tpv"] / df["cum_vol"]

# ================= ATR =================
def calculate_atr(df, period):
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ================= COOLDOWN =================
def has_recent_spike(series, bars):
    if bars == 0:
        return False
    return series[-bars:].any()

# ================= LIQUID SYMBOLS =================
def get_liquid_futures_symbols():
    tickers = client._request_futures_api(method="get", path="ticker/24hr")

    symbols = []
    for t in tickers:
        symbol = t["symbol"]

        if not symbol.endswith("USDT"):
            continue

        if symbol in BLACKLIST:
            continue  # 🔪 режем сразу

        if float(t["quoteVolume"]) < MIN_24H_VOLUME:
            continue

        symbols.append(symbol)

    return symbols

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

    # ===== EMA / ATR / VWAP =====
    df["ema20"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["atr"] = calculate_atr(df, ATR_LEN)
    df["vwap"] = calculate_session_vwap(df)

    # ===== Volume =====
    df["quote_volume"] = df["close"] * df["volume"]
    avg_vol = df["quote_volume"].iloc[-(VOLUME_LOOKBACK+2):-2].mean()
    last = df.iloc[-2]

    spike_trend = last["quote_volume"] >= avg_vol * VOL_MULT_TREND
    spike_counter = last["quote_volume"] >= avg_vol * VOL_MULT_COUNTER

    recent_spike = (
        has_recent_spike(df["quote_volume"][:-2] >= avg_vol * VOL_MULT_TREND, COOLDOWN_BARS) or
        has_recent_spike(df["quote_volume"][:-2] >= avg_vol * VOL_MULT_COUNTER, COOLDOWN_BARS)
    )

    # ===== Candle =====
    body = abs(last["close"] - last["open"])
    rng = last["high"] - last["low"]
    body_pct = 0 if rng == 0 else body / rng * 100

    bull = last["close"] > last["open"]
    bear = last["close"] < last["open"]

    strong_body_trend = body_pct >= MIN_BODY_TREND
    strong_body_counter = body_pct >= MIN_BODY_COUNTER

    # ===== PRICE vs EMA/VWAP (1:1 TV) =====
    below_ema20 = last["open"] < last["ema20"] and last["close"] < last["ema20"]
    above_ema20 = last["open"] > last["ema20"] and last["close"] > last["ema20"]

    below_vwap = last["open"] < last["vwap"] and last["close"] < last["vwap"]
    above_vwap = last["open"] > last["vwap"] and last["close"] > last["vwap"]

    buy_low_condition = last["low"] < last["ema20"] and last["low"] < last["ema200"]
    sell_high_condition = last["high"] > last["ema20"] and last["high"] > last["ema200"]

    bull_trend = last["ema20"] > last["ema200"]
    bear_trend = last["ema20"] < last["ema200"]

    atr = last["atr"]

    emas_far_enough = abs(last["ema20"] - last["ema200"]) >= atr * ATR_GAP_MULT
    ema20_far_vwap = abs(last["ema20"] - last["vwap"]) >= atr * EMA20_PROXIMITY_MULT
    ema200_far_vwap = abs(last["ema200"] - last["vwap"]) >= atr * EMA200_PROXIMITY_MULT
    ema20_far_ema200 = abs(last["ema20"] - last["ema200"]) >= atr * EMA20_PROXIMITY_MULT

    ema20_clear_zone = ema20_far_vwap and ema20_far_ema200 and ema200_far_vwap

    signals = []

    # ===== TREND =====
    if (spike_trend and bull and strong_body_trend and below_ema20 and below_vwap and
        bull_trend and emas_far_enough and buy_low_condition and ema20_far_vwap and not recent_spike):
        signals.append("BUY_TREND")

    if (spike_trend and bear and strong_body_trend and above_ema20 and above_vwap and
        bear_trend and emas_far_enough and sell_high_condition and ema20_far_vwap and not recent_spike):
        signals.append("SELL_TREND")

    # ===== COUNTER =====
    if (spike_counter and bull and strong_body_counter and below_ema20 and below_vwap and
        bear_trend and emas_far_enough and ema20_clear_zone and not recent_spike):
        signals.append("BUY_COUNTER")

    if (spike_counter and bear and strong_body_counter and above_ema20 and above_vwap and
        bull_trend and emas_far_enough and ema20_clear_zone and not recent_spike):
        signals.append("SELL_COUNTER")

    if not signals:
        return None

    return {
        "symbol": symbol,
        "signals": signals,
        "close": last["close"],
        "low": last["low"],
        "high": last["high"],
        "ema20": last["ema20"],
        "ema200": last["ema200"],
        "vwap": last["vwap"],
        "volText": f"x{last['quote_volume']/avg_vol:.2f}"
    }

# ================= MAIN =================
def sleep_until_next_5m():
    now = datetime.now(timezone.utc)
    time.sleep(300 - ((now.minute * 60 + now.second) % 300))

def main():
    symbols = get_liquid_futures_symbols()
    print(f"🚀 Старт | Монет: {len(symbols)}")

    while True:
        print("\n🔍 Проверка сигналов...")
        found = 0

        for s in symbols:
            try:
                # --- Пропускаем черный список ---
                if s in BLACKLIST:
                    continue

                res = check_volume_signal(s)
                if res:
                    found += 1

                    # ===== Добавляем в Excel =====
                    now = datetime.now()
                    new_row = {
                        "Дата": now.date().strftime("%Y-%m-%d"),
                        "Время": now.strftime("%H:%M"),
                        "Тикет": res['symbol'],
                        "Сигнал": ','.join(res['signals']),
                        "Импульс": res['volume_ratio']  # обязательно добавь в check_volume_signal!
                    }
                    df_excel = pd.concat([df_excel, pd.DataFrame([new_row])], ignore_index=True)
                    df_excel.to_excel(EXCEL_PATH, index=False, engine="openpyxl")

                    # ===== Telegram и вывод =====
                    msg = (
                        f"🔥 {res['symbol']}\n"
                        f"Тип: {', '.join(res['signals'])}\n"
                        f"Close: {res['close']:.6f}\n"
                        f"EMA20: {res['ema20']:.6f}\n"
                        f"EMA200: {res['ema200']:.6f}\n"
                        f"VWAP: {res['vwap']:.6f}\n"
                        f"VOL {res['volText']}"
                    )
                    print(msg)
                    send_telegram(msg)

            except Exception as e:
                print(f"{s}: {e}")

        print(f"✅ Найдено сигналов: {found}")
        sleep_until_next_5m()