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

    forward_dates = trading_dates[trading_dates >= start][:window + 1]

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

    df = pd.read_csv("data/processed/event_returns.csv")

    car1 = df["car_1"].dropna()
    car5 = df["car_5"].dropna()
    car30 = df["car_30"].dropna()
    car60 = df["car_60"].dropna()

    nan_30 = df[df["car_30"].isna()]

    late_2024_nans = (
        nan_30[pd.to_datetime(nan_30["event_date"]) >= "2024-10-01"].shape[0]
        if nan_30.shape[0] > 0
        else 0
    )

    checks = []

    checks.append(("output CSV has 2664 rows", df.shape[0] == 2664))
    checks.append(("output has car_1 column", "car_1" in df.columns))
    checks.append(("output has car_5 column", "car_5" in df.columns))
    checks.append(("output has car_30 column", "car_30" in df.columns))
    checks.append(("output has car_60 column", "car_60" in df.columns))

    checks.append(("computed + skipped = 2664", computed + skipped == 2664))

    checks.append(("car_1 coverage > 90%", df["car_1"].notna().mean() > 0.90))
    checks.append(("car_5 coverage > 90%", df["car_5"].notna().mean() > 0.90))
    checks.append(("car_30 coverage > 85%", df["car_30"].notna().mean() > 0.85))
    checks.append(("car_30 valid count > 2200", len(car30) > 2200))
    checks.append(("car_60 coverage > 75%", df["car_60"].notna().mean() > 0.75))

    checks.append(("skipped < 300", skipped < 300))

    checks.append(("no infinities in car_1", np.isfinite(car1).all()))
    checks.append(("no infinities in car_5", np.isfinite(car5).all()))
    checks.append(("no infinities in car_30", np.isfinite(car30).all()))
    checks.append(("no infinities in car_60", np.isfinite(car60).all()))

    checks.append(("car_30 std between 0.02 and 0.50", 0.02 < car30.std() < 0.50))
    checks.append(("99%+ of car_30 in [-2, 2]", car30.between(-2, 2).mean() > 0.99))

    checks.append(("extreme positive car_30 (<50 events)", (car30 > 0.5).sum() < 50))
    checks.append(("extreme negative car_30 (<50 events)", (car30 < -0.5).sum() < 50))

    checks.append(("car_1 coverage >= car_5 coverage", df["car_1"].notna().sum() >= df["car_5"].notna().sum()))
    checks.append(("car_5 coverage >= car_30 coverage", df["car_5"].notna().sum() >= df["car_30"].notna().sum()))
    checks.append(("car_30 coverage >= car_60 coverage", df["car_30"].notna().sum() >= df["car_60"].notna().sum()))

    checks.append(
        (
            "car_30 NaN not dominated by early events",
            nan_30.shape[0] == 0
            or late_2024_nans / max(nan_30.shape[0], 1) > 0.15
            or nan_30.shape[0] < 100,
        )
    )

    for name, result in checks:
        status = "PASS" if result else "FAIL"
        print(f"{status} - {name}")

    print("\nBatch summary:")
    print("computed:", computed)
    print("skipped:", skipped)

    print("\nCoverage:")
    for w in [1, 5, 30, 60]:
        col = f"car_{w}"
        valid = df[col].notna().sum()
        missing = df[col].isna().sum()
        pct = valid / df.shape[0] * 100
        print(f"CAR({w}) -> {valid} valid, {missing} missing ({pct:.1f}%)")

    print("\nCAR(30) NaN breakdown:")
    print("total NaN:", nan_30.shape[0])
    print("from 2024-Q4:", late_2024_nans)
    print("earlier:", nan_30.shape[0] - late_2024_nans)

    print("\nBasic distribution stats:")
    for w, series in [(1, car1), (5, car5), (30, car30), (60, car60)]:
        print(
            f"CAR({w})  mean={series.mean():.4f}  median={series.median():.4f}  std={series.std():.4f}  n={len(series)}"
        )

    print("\nOutliers:")
    print("car_30 > +50%:", (car30 > 0.5).sum())
    print("car_30 < -50%:", (car30 < -0.5).sum())
    print("car_30 > +100%:", (car30 > 1.0).sum())
    print("car_30 < -100%:", (car30 < -1.0).sum())