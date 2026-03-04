import pandas as pd
import numpy as np

events = pd.read_csv("data/processed/raw_cluster_events.csv", parse_dates=["event_date"])

print(f"Total raw cluster events: {len(events)}")
print(f"Unique tickers: {events['ticker'].nunique()}")
print(f"Date range: {events['event_date'].min().date()} to {events['event_date'].max().date()}")
print()
print("Cluster size distribution (unique insiders):")
print(events["n_unique_insiders"].value_counts().sort_index().to_string())
print()

events["year"] = events["event_date"].dt.year
yearly = events.groupby("year").size()
print("Events per year:")
print(yearly.to_string())
print()

# Top tickers by cluster count 
top_tickers = events["ticker"].value_counts().head(15)
print("Top 15 tickers by cluster count:")
print(top_tickers.to_string())
print()

# Spot-check: picked 3 random events and inspect 
print("="*60)
print("SPOT CHECK: 3 random events ")
print("="*60)
sample = events.sample(3, random_state=42)
for _, row in sample.iterrows():
    print(f"\n  Ticker:      {row['ticker']}")
    print(f"  Company:     {row.get('company_name', 'N/A')}")
    print(f"  Event date:  {row['event_date'].date()}")
    print(f"  Insiders:    {row['n_unique_insiders']}")
    print(f"  Trades:      {row['n_transactions']}")
    print(f"  Names:       {row.get('insider_names', 'N/A')}")
    print(f"  Total value: ${row.get('total_value', 0):,.0f}")
    
    # Key check: are these actually different people?
    names = str(row.get("insider_names", "")).split("; ")
    if len(names) != len(set(names)):
        print(f"  [WARNING] Duplicate names detected in insider list!")
    else:
        print(f"  [OK] All {len(names)} insiders are unique")

print()

#Check: window span (days between first and last trade) 
events["cluster_start"] = pd.to_datetime(events["cluster_start"])
events["cluster_end"] = pd.to_datetime(events["cluster_end"])
events["span_days"] = (events["cluster_end"] - events["cluster_start"]).dt.days

print("Cluster span (days between first and last trade in cluster):")
print(f"  Min:    {events['span_days'].min()} days")
print(f"  Median: {events['span_days'].median():.0f} days")
print(f"  Max:    {events['span_days'].max()} days")

if events["span_days"].max() > 30:
    print(f"  [WARNING] Max span > 30 days! Check window logic.")
else:
    print(f"  [OK] All clusters fit within 30-day window.")

print()

#Total dollar value 
if "total_value" in events.columns:
    print("Total cluster purchase value:")
    print(f"  Mean:   ${events['total_value'].mean():,.0f}")
    print(f"  Median: ${events['total_value'].median():,.0f}")
    print(f"  Max:    ${events['total_value'].max():,.0f}")

print("\n[VALIDATION COMPLETE]")