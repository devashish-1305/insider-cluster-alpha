import pandas as pd
import numpy as np


def load_events():
    return pd.read_csv("data/processed/events.csv", parse_dates=["event_date"])


def load_prices():
    return pd.read_csv("data/raw/price_data.csv", index_col=0, parse_dates=True)


def load_spy():
    spy = pd.read_csv("data/raw/spy_prices.csv", index_col=0, parse_dates=True)
    if isinstance(spy, pd.DataFrame):
        spy = spy.squeeze()
    return spy


def compute_returns(prices):
    return prices.pct_change()


def compute_spy_returns(spy):
    return spy.pct_change()


def find_start_idx(date, trading_dates):
    mask = trading_dates >= date
    if mask.sum() == 0:
        return None
    return trading_dates[mask][0]


def compute_car(ticker, event_date, stock_rets, spy_rets, window):
    trading_dates = stock_rets.index
    start = find_start_idx(event_date, trading_dates)
    if start is None:
        return np.nan
    forward_dates = trading_dates[trading_dates >= start][: window + 1]
    if len(forward_dates) < window + 1:
        return np.nan
    if ticker not in stock_rets.columns:
        return np.nan
    stock_slice = stock_rets.loc[forward_dates, ticker]
    spy_slice = spy_rets.loc[forward_dates]
    if stock_slice.isna().any() or spy_slice.isna().any():
        return np.nan
    stock_cum = (1 + stock_slice).prod() - 1
    spy_cum = (1 + spy_slice).prod() - 1
    return stock_cum - spy_cum


def compute_all_cars(events, stock_rets, spy_rets):
    windows = [1, 5, 30, 60]
    results = {f"car_{w}": [] for w in windows}
    skipped = 0
    computed = 0
    for _, row in events.iterrows():
        ticker = row["ticker"]
        event_date = row["event_date"]
        cars = {}
        for w in windows:
            cars[f"car_{w}"] = compute_car(ticker, event_date, stock_rets, spy_rets, w)
        for w in windows:
            results[f"car_{w}"].append(cars[f"car_{w}"])
        if all(np.isnan(v) for v in cars.values()):
            skipped += 1
        else:
            computed += 1
    out = events.copy()
    for col in results:
        out[col] = results[col]
    return out, computed, skipped


if __name__ == "__main__":
    events = load_events()
    prices = load_prices()
    spy = load_spy()
    stock_rets = compute_returns(prices)
    spy_rets = compute_spy_returns(spy)
    results, computed, skipped = compute_all_cars(events, stock_rets, spy_rets)
    results.to_csv("data/processed/event_returns.csv", index=False)