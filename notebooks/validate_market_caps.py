import pandas as pd
import numpy as np

df = pd.read_csv("data/raw/market_caps.csv")

valid = df[df["market_cap"].notna()].copy()
valid["mktcap_B"] = valid["market_cap"] / 1e9

above_300m = valid[valid["market_cap"] >= 300_000_000]
below_300m = valid[valid["market_cap"] < 300_000_000]

known = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM", "WMT", "XOM", "PFE", "BA", "DIS"]
check = df[df["ticker"].isin(known)].sort_values("market_cap", ascending=False)

suspicious_high = valid[valid["market_cap"] > 5e12]
suspicious_low = valid[valid["market_cap"] < 1e6]