from binance.client import Client
import pandas as pd
import time
from datetime import datetime, timezone
import requests
import os
import json
import argparse
from dotenv import load_dotenv

# ===== ЗАГРУЗКА КОНФИГА =====
parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
args = parser.parse_args()

with open(args.config, "r") as f:
    cfg = json.load(f)

BOT_NAME = cfg["NAME"]

load_dotenv()

# ================= НАСТРОЙКИ =================
MIN_24H_VOLUME = cfg["MIN_24H_VOLUME"]
LOOKBACK_CANDLES = cfg["LOOKBACK_CANDLES"]
VOLUME_LOOKBACK = cfg["VOLUME_LOOKBACK"]

VOL_MULT_TREND = float(cfg["VOL_MULT_TREND"])
VOL_MULT_COUNTER = float(cfg["VOL_MULT_COUNTER"])

EMA_FAST = cfg["EMA_FAST"]
EMA_SLOW = cfg["EMA_SLOW"]

MIN_BODY_TREND = float(cfg["MIN_BODY_TREND"])
MIN_BODY_COUNTER = float(cfg["MIN_BODY_COUNTER"])

ATR_LEN = cfg["ATR_LEN"]
ATR_GAP_MULT = float(cfg["ATR_GAP_MULT"])
EMA20_PROXIMITY_MULT = float(cfg["EMA20_PROXIMITY_MULT"])
EMA200_PROXIMITY_MULT = float(cfg["EMA200_PROXIMITY_MULT"])

COOLDOWN_BARS = cfg["COOLDOWN_BARS"]
SLEEP = cfg["SLEEP"]

CHAT_ID = os.getenv("CHAT_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")

client = Client()

BLACKLIST = {
    "BTCUSDT",
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

    # ===== PREVIOUS VOLUME CHECK (3 candles before) =====
    prev_candles = df.iloc[-5:-2]      # 3 свечи ДО анализируемой
    last_qv = last["quote_volume"]

    prev_vol_higher_count = int(
        (prev_candles["quote_volume"] > last_qv).sum()
    )

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

    ticker_24h = client.futures_ticker(symbol=symbol)
    volume_24h = float(ticker_24h["quoteVolume"])

    return {
        "symbol": symbol,
        "signals": signals,
        "close": last["close"],
        "low": last["low"],
        "high": last["high"],
        "ema20": last["ema20"],
        "ema200": last["ema200"],
        "vwap": last["vwap"],
        "volText": f"x{last['quote_volume']/avg_vol:.2f}",
        "prevVolCount": prev_vol_higher_count,
        "volume_24h": volume_24h
    }

# ================= MAIN =================
def sleep_until_next_5m():
    now = datetime.now(timezone.utc)
    # Ждем до начала следующей свечи + 3 секунды на запас
    time.sleep(300 - ((now.minute * 60 + now.second) % 300) + 3)

def main():
    symbols = get_liquid_futures_symbols()
    print(f"🚀 Старт | Монет: {len(symbols)}")

    while True:
        print("\n🔍 Проверка сигналов...")

        results = []

        # ===== 1. Собираем все сигналы =====
        for s in symbols:
            try:
                res = check_volume_signal(s)
                if res:
                    results.append(res)
            except Exception as e:
                print(f"{s}: {e}")

        # ===== 2. Считаем COUNTER =====
        counter_count = sum(
            1 for r in results
            if any(sig in ("BUY_COUNTER", "SELL_COUNTER") for sig in r["signals"])
        )

        skip_counters = counter_count >= 2

        if skip_counters:
            print(f"⛔ Найдено {counter_count} COUNTER → counter-сигналы пропущены")

        found = 0

        # ===== 3. Фильтрация + отправка с корреляцией к BTC =====
        # ===== 3. Фильтрация + отправка с корреляцией к BTC =====
        # Загружаем BTCUSDT один раз за цикл (9 часов = 108 свечей по 5 мин)
        try:
            klines_btc = client.futures_klines(
                symbol="BTCUSDT",
                interval=Client.KLINE_INTERVAL_5MINUTE,
                limit=108
            )
            df_btc = pd.DataFrame(klines_btc, columns=[
                "open_time","open","high","low","close",
                "volume","close_time","quote_volume",
                "trades","taker_buy_base","taker_buy_quote","ignore"
            ])
            df_btc["close"] = df_btc["close"].astype(float)
            btc_returns = df_btc["close"].pct_change()
        except Exception as e:
            print(f"Ошибка загрузки BTC свечей: {e}")
            btc_returns = None

        for res in results:
            signals = res["signals"]

            # убираем ТОЛЬКО counter, если их >= 2
            if skip_counters:
                signals = [
                    s for s in signals
                    if s not in ("BUY_COUNTER", "SELL_COUNTER")
                ]

            if not signals:
                continue  # нечего отправлять

            # ===== КОРРЕЛЯЦИЯ С BTC =====
            try:
                if btc_returns is not None:
                    # Берём последние 108 свечей для символа
                    klines_sym = client.futures_klines(
                        symbol=res["symbol"],
                        interval=Client.KLINE_INTERVAL_5MINUTE,
                        limit=108
                    )
                    df_sym = pd.DataFrame(klines_sym, columns=[
                        "open_time","open","high","low","close",
                        "volume","close_time","quote_volume",
                        "trades","taker_buy_base","taker_buy_quote","ignore"
                    ])
                    df_sym["close"] = df_sym["close"].astype(float)
                    symbol_returns = df_sym["close"].pct_change()

                    # Совпадающие длины
                    btc_subset = btc_returns[-len(symbol_returns):]

                    corr = btc_subset.corr(symbol_returns)
                    corr_text = f"{corr:.2f}" if corr is not None else "N/A"
                else:
                    corr_text = "N/A"
            except Exception as e:
                print(f"Ошибка при расчёте корреляции для {res['symbol']}: {e}")
                corr_text = "N/A"

            # ===== Формируем сообщение =====
            vol24 = res["volume_24h"] / 1_000_000
            msg = (
                f"🤖 {BOT_NAME}\n"
                f"🔥 {res['symbol']}\n"
                f"Тип: {', '.join(signals)}\n"
                f"Close: {res['close']:.6f}\n"
                f"EMA20: {res['ema20']:.6f}\n"
                f"EMA200: {res['ema200']:.6f}\n"
                f"VWAP: {res['vwap']:.6f}\n"
                f"VOL {res['volText']}\n"
                f"Prev volume higher: {res['prevVolCount']}/3\n"
                f"VOL 24h: {vol24:.1f}M USDT\n"
                f"Corr BTC: {corr_text}\n"
            )

            print(msg)
            send_telegram(msg)

        print(f"✅ Отправлено сигналов: {found}")
        sleep_until_next_5m()

if __name__ == "__main__":
    main()
