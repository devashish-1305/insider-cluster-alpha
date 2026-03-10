import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path


def load_event_returns():
    return pd.read_csv('data/processed/event_returns.csv')


def compute_window_stats(cars):
    clean = cars.dropna()
    n = len(clean)
    mean = clean.mean()
    median = clean.median()
    std = clean.std()
    t_stat, p_value = stats.ttest_1samp(clean, 0)
    hit_rate = (clean > 0).mean()
    se = std / np.sqrt(n)
    ci_lower = mean - 1.96 * se
    ci_upper = mean + 1.96 * se
    return {
        'n': n, 'mean': mean, 'median': median, 'std': std,
        't_stat': t_stat, 'p_value': p_value, 'hit_rate': hit_rate,
        'ci_lower': ci_lower, 'ci_upper': ci_upper
    }


def build_summary(df):
    windows = {'CAR(0,1)': 'car_1', 'CAR(0,5)': 'car_5',
               'CAR(0,30)': 'car_30', 'CAR(0,60)': 'car_60'}
    rows = []
    for label, col in windows.items():
        row = compute_window_stats(df[col])
        row['window'] = label
        row['significant'] = row['p_value'] < 0.0125
        rows.append(row)
    return pd.DataFrame(rows)


def generate_interpretation(summary):
    car30 = summary[summary['window'] == 'CAR(0,30)'].iloc[0]
    direction = 'positive' if car30['mean'] > 0 else 'negative'
    sig = 'statistically significant' if car30['significant'] else 'not statistically significant'
    hit_desc = 'above' if car30['hit_rate'] > 0.5 else 'at or below'
    majority = 'a majority' if car30['hit_rate'] > 0.5 else 'fewer than half'

    s1 = (f"The primary pre-registered test, CAR(0,30), shows a mean abnormal return of "
          f"{car30['mean']:.2%} (t={car30['t_stat']:.3f}, p={car30['p_value']:.6f}), which is "
          f"{sig} at the Bonferroni-adjusted threshold of 0.0125.")

    s2 = (f"The hit rate of {car30['hit_rate']:.1%} is {hit_desc} 50%, indicating that "
          f"{majority} of cluster events are followed by {direction} abnormal returns over "
          f"30 trading days.")

    sig_windows = summary[summary['significant']]['window'].tolist()
    if sig_windows:
        s3 = f"Significant abnormal returns are observed at: {', '.join(sig_windows)}."
    else:
        s3 = "No horizon reaches statistical significance after Bonferroni correction."

    return [s1, s2, s3]


def write_results_md(summary):
    lines = ['# Insider Cluster Alpha Study — Results', '',
             '## Primary Statistical Tests', '',
             'Bonferroni-adjusted significance threshold: p < 0.0125', '',
             '| Window | N | Mean CAR | Median CAR | Std Dev | t-stat | p-value | Hit Rate | 95% CI | Significant |',
             '|--------|---|----------|------------|---------|--------|---------|----------|--------|-------------|']

    for _, r in summary.iterrows():
        ci = f"[{r['ci_lower']:.4f}, {r['ci_upper']:.4f}]"
        sig = 'Yes' if r['significant'] else 'No'
        lines.append(f"| {r['window']} | {int(r['n'])} | {r['mean']:.4f} | {r['median']:.4f} | "
                     f"{r['std']:.4f} | {r['t_stat']:.3f} | {r['p_value']:.6f} | "
                     f"{r['hit_rate']:.3f} | {ci} | {sig} |")

    lines += ['', '## Interpretation', '']
    for sentence in generate_interpretation(summary):
        lines.append(sentence)
        lines.append('')

    with open('results.md', 'w') as f:
        f.write('\n'.join(lines))


if __name__ == '__main__':
    df = load_event_returns()
    summary = build_summary(df)
    write_results_md(summary)

    assert len(summary) == 4
    for col in ['n', 'mean', 'median', 'std', 't_stat', 'p_value', 'hit_rate']:
        assert col in summary.columns
    assert summary['n'].min() > 100
    assert all(summary['p_value'].between(0, 1))
    assert all(summary['hit_rate'].between(0, 1))
    assert all(summary['std'] > 0)
    n_vals = summary.set_index('window')['n']
    assert n_vals['CAR(0,1)'] >= n_vals['CAR(0,60)']
    car30_mean = summary.loc[summary['window'] == 'CAR(0,30)', 'mean'].values[0]
    assert -0.5 < car30_mean < 0.5
    assert Path('results.md').exists()
    assert Path('results.md').stat().st_size > 500

    print(summary[['window', 'n', 'mean', 'median', 't_stat', 'p_value',
                    'hit_rate', 'significant']].to_string(index=False))
    print()
    car30 = summary[summary['window'] == 'CAR(0,30)'].iloc[0]
    print(f"PRIMARY TEST — CAR(0,30): mean={car30['mean']:.4f}, "
          f"t={car30['t_stat']:.3f}, p={car30['p_value']:.6f}, "
          f"hit_rate={car30['hit_rate']:.3f}, "
          f"significant={'YES' if car30['significant'] else 'NO'}")
    print()
    print('results.md written — all checks passed')