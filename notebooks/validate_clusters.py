import pandas as pd
import numpy as np

events = pd.read_csv("data/processed/raw_cluster_events.csv", parse_dates=["event_date"])

events["year"] = events["event_date"].dt.year
yearly = events.groupby("year").size()

top_tickers = events["ticker"].value_counts().head(15)

sample = events.sample(3, random_state=42)
for _, row in sample.iterrows():
    names = str(row.get("insider_names", "")).split("; ")

events["cluster_start"] = pd.to_datetime(events["cluster_start"])
events["cluster_end"] = pd.to_datetime(events["cluster_end"])
events["span_days"] = (events["cluster_end"] - events["cluster_start"]).dt.days