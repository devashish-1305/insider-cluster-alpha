import pandas as pd
import numpy as np

df = pd.read_csv("data/raw/market_caps.csv")

print(f"Total rows:          {len(df)}")
print(f"Non-null market cap: {df['market_cap'].notna().sum()}")
print(f"Null (failed):       {df['market_cap'].isna().sum()}")
print()

valid = df[df["market_cap"].notna()].copy()
valid["mktcap_B"] = valid["market_cap"] / 1e9

print("Market Cap Distribution (in $B):")
print(f"  Min:    ${valid['mktcap_B'].min():.3f}B")
print(f"  25th:   ${valid['mktcap_B'].quantile(0.25):.3f}B")
print(f"  Median: ${valid['mktcap_B'].quantile(0.50):.3f}B")
print(f"  75th:   ${valid['mktcap_B'].quantile(0.75):.3f}B")
print(f"  Max:    ${valid['mktcap_B'].max():.1f}B")
print()

#How many pass the \$300M filter? 
above_300m = valid[valid["market_cap"] >= 300_000_000]
below_300m = valid[valid["market_cap"] < 300_000_000]

print(f"Market cap >= \$300M: {len(above_300m)} tickers")
print(f"Market cap <  \$300M: {len(below_300m)} tickers")
print()

# Spot-check: show 10 well-known tickers
known = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM", "WMT", "XOM", "PFE", "BA", "DIS"]
check = df[df["ticker"].isin(known)].sort_values("market_cap", ascending=False)
print("Spot-check (known tickers):")
for _, row in check.iterrows():
    mktcap_str = f"${row['market_cap']/1e9:.1f}B" if pd.notna(row["market_cap"]) else "MISSING"
    print(f"  {row['ticker']:6s} → {mktcap_str}")
print()

# Sanity: flag anything suspiciously large or small 
suspicious_high = valid[valid["market_cap"] > 5e12]  # > \$5T
suspicious_low = valid[valid["market_cap"] < 1e6]    # < \$1M
if len(suspicious_high) > 0:
    print(f"[WARNING] {len(suspicious_high)} tickers above \$5T — check these:")
    print(suspicious_high[["ticker", "mktcap_B"]].to_string(index=False))
if len(suspicious_low) > 0:
    print(f"[WARNING] {len(suspicious_low)} tickers below \$1M — possibly bad data:")
    print(suspicious_low[["ticker", "market_cap"]].head(10).to_string(index=False))

print("\n[VALIDATION COMPLETE]")