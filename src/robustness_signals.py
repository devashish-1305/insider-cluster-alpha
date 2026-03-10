import pandas as pd
from signal_definition import load_insider_data, detect_all_clusters
from event_filter import load_market_caps, apply_overlap_dedup
from event_study import load_prices, load_spy, compute_returns, compute_spy_returns, compute_all_cars
from evaluation import build_summary


def filter_events(raw_events, market_caps):
    raw_events["event_date"] = pd.to_datetime(raw_events["event_date"]).dt.normalize()
    merged = raw_events.merge(market_caps, on="ticker", how="left")
    merged = merged.dropna(subset=["market_cap"])
    merged = merged[merged["market_cap"] >= 300_000_000].copy()
    return apply_overlap_dedup(merged)


def run_variant(insider_df, market_caps, stock_rets, spy_rets, min_insiders, window_days, label):
    raw = detect_all_clusters(insider_df, window_days=window_days, min_insiders=min_insiders)
    if len(raw) == 0:
        return pd.DataFrame()
    filtered = filter_events(raw, market_caps)
    if len(filtered) == 0:
        return pd.DataFrame()
    results, _, _ = compute_all_cars(filtered, stock_rets, spy_rets)
    summary = build_summary(results)
    summary["variant"] = label
    summary["num_events"] = len(filtered)
    return summary


def write_markdown(combined):
    cols = ["variant", "num_events", "window", "n", "mean", "median", "std",
            "t_stat", "p_value", "hit_rate", "ci_lower", "ci_upper", "significant"]
    combined = combined[cols]
    lines = ["# Robustness: Alternative Signal Definitions\n"]
    for label in combined["variant"].unique():
        subset = combined[combined["variant"] == label]
        n_events = int(subset["num_events"].iloc[0])
        lines.append(f"## {label} (N={n_events} events)\n")
        lines.append("| Window | N | Mean | Median | Std | t | p | Hit Rate | 95% CI | Bonferroni Sig |")
        lines.append("|--------|---|------|--------|-----|---|---|----------|--------|----------------|")
        for _, row in subset.iterrows():
            sig = "Yes" if row["significant"] else "No"
            lines.append(
                f"| {row['window']} "
                f"| {int(row['n'])} "
                f"| {row['mean']:.4f} "
                f"| {row['median']:.4f} "
                f"| {row['std']:.4f} "
                f"| {row['t_stat']:.3f} "
                f"| {row['p_value']:.2e} "
                f"| {row['hit_rate']:.3f} "
                f"| [{row['ci_lower']:.4f}, {row['ci_upper']:.4f}] "
                f"| {sig} |"
            )
        lines.append("")
    with open("results_robustness_signals.md", "w") as f:
        f.write("\n".join(lines))


def main():
    insider_df = load_insider_data()
    market_caps = load_market_caps()
    prices = load_prices()
    spy = load_spy()
    stock_rets = compute_returns(prices)
    spy_rets = compute_spy_returns(spy)

    variants = [
        {"min_insiders": 3, "window_days": 30, "label": "baseline (3ins/30d)"},
        {"min_insiders": 2, "window_days": 30, "label": "2ins/30d"},
        {"min_insiders": 5, "window_days": 30, "label": "5ins/30d"},
        {"min_insiders": 3, "window_days": 14, "label": "3ins/14d"},
        {"min_insiders": 3, "window_days": 60, "label": "3ins/60d"},
    ]

    all_results = []
    for v in variants:
        summary = run_variant(
            insider_df, market_caps, stock_rets, spy_rets,
            min_insiders=v["min_insiders"],
            window_days=v["window_days"],
            label=v["label"]
        )
        if len(summary) > 0:
            all_results.append(summary)

    combined = pd.concat(all_results, ignore_index=True)
    combined.to_csv("data/processed/robustness_signal_variants.csv", index=False)
    write_markdown(combined)


if __name__ == "__main__":
    main()