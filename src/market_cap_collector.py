import pandas as pd
import yfinance as yf
import time
import os
import logging


logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def load_tickers_from_prices(price_path: str) -> list:
    df = pd.read_csv(price_path, nrows=0)
    tickers = [col for col in df.columns if col != "Date"]
    return tickers


def load_existing_progress(output_path: str) -> dict:
    if os.path.exists(output_path):
        df = pd.read_csv(output_path)
        existing = dict(zip(df["ticker"], df["market_cap"]))
        return existing
    return {}


def fetch_market_cap(ticker_symbol: str) -> float:
    try:
        tk = yf.Ticker(ticker_symbol)
        info = tk.info

        mktcap = info.get("marketCap", None)

        if mktcap is not None and mktcap > 0:
            return float(mktcap)

        ev = info.get("enterpriseValue", None)
        if ev is not None and ev > 0:
            return float(ev)

        return None

    except Exception as e:
        return None


def collect_market_caps(
    price_path: str = "data/raw/price_data.csv",
    output_path: str = "data/raw/market_caps.csv",
    sleep_between: float = 0.5,
    save_every: int = 50
):
    tickers = load_tickers_from_prices(price_path)
    collected = load_existing_progress(output_path)

    remaining = [t for t in tickers if t not in collected]

    failed = []
    new_count = 0

    for i, ticker in enumerate(remaining):
        mktcap = fetch_market_cap(ticker)

        if mktcap is not None:
            collected[ticker] = mktcap
        else:
            collected[ticker] = None
            failed.append(ticker)

        new_count += 1

        if new_count % save_every == 0:
            _save_checkpoint(collected, output_path)

        time.sleep(sleep_between)

    _save_checkpoint(collected, output_path)

    if failed:
        fail_df = pd.DataFrame({"ticker": failed})
        fail_path = "data/raw/failed_mktcap_tickers.csv"
        fail_df.to_csv(fail_path, index=False)

    return collected


def _save_checkpoint(collected: dict, output_path: str):
    df = pd.DataFrame([
        {"ticker": t, "market_cap": v}
        for t, v in collected.items()
    ])
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    collect_market_caps()