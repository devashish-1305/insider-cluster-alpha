import numpy as np
import pandas as pd

try:
    from src.event_study import load_events, load_prices, load_spy, compute_returns, compute_spy_returns, find_start_idx
    from src.evaluation import build_summary, generate_interpretation
except ImportError:
    from event_study import load_events, load_prices, load_spy, compute_returns, compute_spy_returns, find_start_idx
    from evaluation import build_summary, generate_interpretation


WINDOWS = [1, 5, 30, 60]
EST_PRE_DAYS = 250
EST_GAP_DAYS = 30
MIN_EST_OBS = 120


def estimate_capm(est_stock, est_spy):
    valid = pd.concat([est_stock, est_spy], axis=1).dropna()
    if len(valid) < MIN_EST_OBS:
        return np.nan, np.nan
    y = valid.iloc[:, 0].to_numpy(dtype=float)
    x = valid.iloc[:, 1].to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(x)), x])
    coef = np.linalg.lstsq(X, y, rcond=None)[0]
    return float(coef[0]), float(coef[1])


def compute_capm_cars_for_event(ticker, idx0, stock_rets, spy_rets):
    out = {"capm_alpha": np.nan, "capm_beta": np.nan}
    for w in WINDOWS:
        out[f"capm_car_{w}"] = np.nan

    if idx0 is None:
        return out

    est_start = idx0 - EST_PRE_DAYS
    est_end = idx0 - EST_GAP_DAYS

    if est_start < 0 or est_end <= est_start:
        return out

    if ticker not in stock_rets.columns:
        return out

    est_stock = stock_rets[ticker].iloc[est_start:est_end + 1]
    est_spy = spy_rets.iloc[est_start:est_end + 1]

    alpha, beta = estimate_capm(est_stock, est_spy)
    if np.isnan(alpha) or np.isnan(beta):
        return out

    out["capm_alpha"] = alpha
    out["capm_beta"] = beta

    for w in WINDOWS:
        ev_start = idx0
        ev_end = idx0 + w
        if ev_end >= len(stock_rets.index):
            continue

        ev_stock = stock_rets[ticker].iloc[ev_start:ev_end + 1]
        ev_spy = spy_rets.iloc[ev_start:ev_end + 1]
        if ev_stock.isna().any() or ev_spy.isna().any():
            continue

        expected = alpha + beta * ev_spy
        abnormal = ev_stock - expected
        out[f"capm_car_{w}"] = float(abnormal.sum())

    return out


def build_compare_table(baseline, capm_eval):
    rows = []
    for w in WINDOWS:
        col = f"car_{w}"
        rows.append({
            "window": f"CAR(0,{w})",
            "market_adjusted_mean": round(float(baseline[col].dropna().mean()), 6),
            "capm_adjusted_mean": round(float(capm_eval[col].dropna().mean()), 6),
            "market_adjusted_n": int(baseline[col].dropna().shape[0]),
            "capm_adjusted_n": int(capm_eval[col].dropna().shape[0]),
        })
    return pd.DataFrame(rows)


def build_coverage_lines(capm_df, total):
    lines = []
    computed = int(capm_df["capm_alpha"].notna().sum())
    skipped = total - computed
    lines.append(f"Total events: {total}")
    lines.append(f"CAPM estimated (enough history): {computed} ({100*computed/total:.1f}%)")
    lines.append(f"Skipped (insufficient history): {skipped} ({100*skipped/total:.1f}%)")
    for w in WINDOWS:
        col = f"capm_car_{w}"
        valid = int(capm_df[col].notna().sum())
        lines.append(f"CAPM CAR(0,{w}) valid: {valid} ({100*valid/total:.1f}%)")
    return lines


def write_results_md(path, title, summary, interpretation, coverage_lines, compare_table):
    lines = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append("## Coverage")
    lines.append("")
    for c in coverage_lines:
        lines.append(f"- {c}")
    lines.append("")
    lines.append("## Summary statistics (t-test vs 0)")
    lines.append("")
    lines.append(summary.to_markdown(index=False))
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    for s in interpretation:
        lines.append(f"- {s}")
    lines.append("")
    lines.append("## Market-adjusted vs CAPM-adjusted comparison")
    lines.append("")
    lines.append(compare_table.to_markdown(index=False))
    lines.append("")
    text = "\n".join(lines).strip() + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def main():
    events = load_events().copy()
    prices = load_prices()
    spy = load_spy()

    stock_rets = compute_returns(prices)
    spy_rets = compute_spy_returns(spy)
    trading_dates = stock_rets.index

    events["event_date"] = pd.to_datetime(events["event_date"])

    rows = []
    for _, r in events.iterrows():
        ticker = str(r["ticker"])
        event_date = pd.to_datetime(r["event_date"])
        idx0_ts = find_start_idx(event_date, trading_dates)
        idx0 = None if idx0_ts is None else int(trading_dates.get_loc(idx0_ts))
        caps = compute_capm_cars_for_event(ticker, idx0, stock_rets, spy_rets)
        rows.append({"ticker": ticker, "event_date": event_date, **caps})

    capm_df = pd.DataFrame(rows)
    capm_df.to_csv("data/processed/event_returns_capm.csv", index=False)

    total = len(capm_df)
    coverage_lines = build_coverage_lines(capm_df, total)

    capm_eval = capm_df.rename(columns={
        "capm_car_1": "car_1",
        "capm_car_5": "car_5",
        "capm_car_30": "car_30",
        "capm_car_60": "car_60",
    })

    capm_summary = build_summary(capm_eval)
    capm_interp = generate_interpretation(capm_summary)

    baseline = pd.read_csv("data/processed/event_returns.csv")
    compare = build_compare_table(baseline, capm_eval)

    write_results_md(
        "results_capm.md",
        "CAPM-adjusted robustness check",
        capm_summary,
        capm_interp,
        coverage_lines,
        compare,
    )

    assert total == 2664
    assert {"capm_alpha", "capm_beta", "capm_car_30"}.issubset(set(capm_df.columns))
    assert capm_df["capm_car_30"].notna().sum() > 0


if __name__ == "__main__":
    main()