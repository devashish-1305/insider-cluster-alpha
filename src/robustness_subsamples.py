import pandas as pd
from evaluation import build_summary


def load_event_returns():
    df = pd.read_csv("data/processed/event_returns.csv", parse_dates=["event_date"])
    return df


def split_by_period(df):
    early = df[df["event_date"].dt.year <= 2019].copy()
    late = df[df["event_date"].dt.year >= 2020].copy()
    return early, late


def remove_bottom_quartile(df):
    q25 = df["market_cap"].quantile(0.25)
    return df[df["market_cap"] >= q25].copy()


def run_subsample(df, label):
    summary = build_summary(df)
    summary["subsample"] = label
    summary["num_events"] = len(df)
    return summary


def write_markdown(combined):
    lines = ["# Robustness: Subsample Analysis (Time Split + Size Filter)\n"]
    for label in combined["subsample"].unique():
        subset = combined[combined["subsample"] == label]
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
    with open("results_robustness_subsamples.md", "w") as f:
        f.write("\n".join(lines))


def main():
    df = load_event_returns()

    full = run_subsample(df, "full sample (baseline)")

    early, late = split_by_period(df)
    early_summary = run_subsample(early, "2015-2019")
    late_summary = run_subsample(late, "2020-2024")

    top75 = remove_bottom_quartile(df)
    top75_summary = run_subsample(top75, "top 75% market cap")

    combined = pd.concat([full, early_summary, late_summary, top75_summary], ignore_index=True)
    combined.to_csv("data/processed/robustness_subsamples.csv", index=False)
    write_markdown(combined)


if __name__ == "__main__":
    main()