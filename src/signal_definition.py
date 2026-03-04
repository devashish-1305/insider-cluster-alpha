import pandas as pd
import numpy as np
from typing import List, Dict, Tuple


def load_insider_data(path: str = "data/processed/insider_cleaned.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    
    # Ensure dates are datetime
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    
    # Sort by ticker then trade_date for the sliding window logic
    df = df.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    
    print(f"[INFO] Loaded {len(df)} insider purchase rows")
    print(f"[INFO] {df['ticker'].nunique()} unique tickers")
    print(f"[INFO] Date range: {df['trade_date'].min().date()} to {df['trade_date'].max().date()}")
    
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
        
        # Grab all transactions in [anchor_date, anchor_date + 30 days]
        mask = (rows["trade_date"] >= anchor_date) & (rows["trade_date"] <= window_end)
        window_rows = rows[mask]
        
        # Count unique insiders
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
    total = len(tickers)
    
    print(f"\n[INFO] Running cluster detection on {total} tickers...")
    print(f"[INFO] Parameters: window={window_days} days, min_insiders={min_insiders}")
    print()
    
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
        
        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{total} tickers — {len(all_events)} clusters found so far")
    
    events_df = pd.DataFrame(all_events)
    
    if len(events_df) > 0:
        events_df = events_df.sort_values("event_date").reset_index(drop=True)
    
    print(f"\n{'='*60}")
    print(f"[DONE] Cluster detection complete")
    print(f"  Total tickers scanned:   {total}")
    print(f"  Total cluster events:    {len(events_df)}")
    if len(events_df) > 0:
        print(f"  Unique tickers w/events: {events_df['ticker'].nunique()}")
        print(f"  Date range:              {events_df['event_date'].min().date()} to {events_df['event_date'].max().date()}")
        print(f"  Avg insiders/cluster:    {events_df['n_unique_insiders'].mean():.1f}")
        print(f"  Avg transactions/cluster:{events_df['n_transactions'].mean():.1f}")
    print(f"{'='*60}")
    
    return events_df
def save_raw_events(events_df: pd.DataFrame, path: str = "data/processed/raw_cluster_events.csv"):
    """Save raw (unfiltered) cluster events to disk."""
    events_df.to_csv(path, index=False)
    print(f"\n[SAVED] {len(events_df)} raw cluster events → {path}")

if __name__ == "__main__":
    df = load_insider_data("data/processed/insider_cleaned.csv")
    
   
    events = detect_all_clusters(df, window_days=30, min_insiders=3)
    
    save_raw_events(events, "data/processed/raw_cluster_events.csv")
    
    if len(events) > 0:
        print("\n[PREVIEW] First 10 events:")
        preview_cols = ["ticker", "event_date", "n_unique_insiders", 
                        "n_transactions", "total_value", "insider_names"]
        preview_cols = [c for c in preview_cols if c in events.columns]
        print(events[preview_cols].head(10).to_string(index=False))
        
        print(f"\n[PREVIEW] Last 10 events:")
        print(events[preview_cols].tail(10).to_string(index=False))