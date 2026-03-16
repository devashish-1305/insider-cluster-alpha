import pandas as pd
import numpy as np
from typing import List, Dict


def load_insider_data(path: str = "data/processed/insider_cleaned.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    return df


def detect_clusters_for_ticker(
    ticker_df: pd.DataFrame,
    window_days: int = 30,
    min_insiders: int = 3
) -> List[Dict]:
    events = []
    rows = ticker_df.reset_index(drop=True)
    n = len(rows)
    i = 0

    while i < n:
        anchor_date = rows.loc[i, "trade_date"]
        window_end = anchor_date + pd.Timedelta(days=window_days)

        mask = (rows["trade_date"] >= anchor_date) & (rows["trade_date"] <= window_end)
        window_rows = rows[mask]

        unique_insiders = window_rows["insider_name"].nunique()

        if unique_insiders >= min_insiders:
            event_date = window_rows["filing_date"].max()

            event = {
                "ticker": rows.loc[i, "ticker"],
                "company_name": rows.loc[i, "company_name"],
                "event_date": event_date,
                "cluster_start": anchor_date,
                "cluster_end": window_rows["trade_date"].max(),
                "n_unique_insiders": unique_insiders,
                "n_transactions": len(window_rows),
                "insider_names": "; ".join(sorted(window_rows["insider_name"].unique())),
                "total_value": window_rows["value"].sum() if "value" in window_rows.columns else np.nan,
                "avg_price": window_rows["price"].mean() if "price" in window_rows.columns else np.nan,
                "first_trade_date": anchor_date,
                "last_trade_date": window_rows["trade_date"].max(),
                "first_filing_date": window_rows["filing_date"].min(),
                "last_filing_date": window_rows["filing_date"].max(),
            }
            events.append(event)

            last_window_idx = window_rows.index[-1]
            i = last_window_idx + 1
        else:
            i += 1

    return events


def detect_all_clusters(
    df: pd.DataFrame,
    window_days: int = 30,
    min_insiders: int = 3
) -> pd.DataFrame:
    all_events = []
    tickers = df["ticker"].unique()

    for idx, ticker in enumerate(tickers):
        ticker_df = df[df["ticker"] == ticker].copy()
        if ticker_df["insider_name"].nunique() < min_insiders:
            continue

        events = detect_clusters_for_ticker(
            ticker_df,
            window_days=window_days,
            min_insiders=min_insiders
        )
        all_events.extend(events)

    events_df = pd.DataFrame(all_events)

    if len(events_df) > 0:
        events_df = events_df.sort_values("event_date").reset_index(drop=True)

    return events_df


def save_raw_events(events_df: pd.DataFrame, path: str = "data/processed/raw_cluster_events.csv"):
    events_df.to_csv(path, index=False)


if __name__ == "__main__":
    df = load_insider_data("data/processed/insider_cleaned.csv")
    events = detect_all_clusters(df, window_days=30, min_insiders=3)
    save_raw_events(events, "data/processed/raw_cluster_events.csv")