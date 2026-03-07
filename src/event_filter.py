import pandas as pd
from pathlib import Path


def load_raw_events(path="data/processed/raw_cluster_events.csv"):
    df = pd.read_csv(path)
    date_cols = ["event_date", "cluster_start", "cluster_end",
                 "first_trade_date", "last_trade_date",
                 "first_filing_date", "last_filing_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    df["event_date"] = df["event_date"].dt.normalize()
    return df


def load_market_caps(path="data/raw/market_caps.csv"):
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if df["market_cap"].dtype == object:
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df = df.dropna(subset=["market_cap"])
    df = df[df["market_cap"] > 0].copy()
    return df[["ticker", "market_cap"]]


def apply_overlap_dedup(events, min_gap_days=60):
    events = events.sort_values(["ticker", "event_date"]).reset_index(drop=True)
    keep = []
    for ticker, group in events.groupby("ticker"):
        group = group.sort_values("event_date")
        last_kept = None
        for idx, row in group.iterrows():
            if last_kept is None or (row["event_date"] - last_kept).days > min_gap_days:
                keep.append(idx)
                last_kept = row["event_date"]
    return events.loc[keep].reset_index(drop=True)


def main():
    raw_events = load_raw_events()
    mktcaps = load_market_caps()
    events = raw_events.merge(mktcaps, on="ticker", how="left")
    events = events[events["market_cap"] >= 300_000_000].copy()
    events = apply_overlap_dedup(events)
    out = Path("data/processed/events.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(out, index=False)
    return events


if __name__ == "__main__":
    events = main()
