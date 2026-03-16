import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path


def load_event_returns():
    return pd.read_csv('data/processed/event_returns.csv')


def plot_car_curve(df):
    Path('paper/figures').mkdir(parents=True, exist_ok=True)

    cols = ['car_1', 'car_5', 'car_30', 'car_60']
    days = [0, 1, 5, 30, 60]
    means = [0.0]
    lowers = [0.0]
    uppers = [0.0]

    for col in cols:
        clean = df[col].dropna()
        m = clean.mean()
        se = clean.std() / np.sqrt(len(clean))
        means.append(m)
        lowers.append(m - 1.96 * se)
        uppers.append(m + 1.96 * se)

    means_pct = [x * 100 for x in means]
    lowers_pct = [x * 100 for x in lowers]
    uppers_pct = [x * 100 for x in uppers]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(days, means_pct, 'b-o', linewidth=2, markersize=8, zorder=3)
    ax.fill_between(days, lowers_pct, uppers_pct, alpha=0.2, color='blue', label='95% CI')
    ax.axhline(0, color='red', linestyle='--', linewidth=1)

    for i, d in enumerate(days):
        if i == 0:
            continue
        ax.annotate(f'{means_pct[i]:.2f}%', (d, means_pct[i]),
                    textcoords='offset points', xytext=(0, 12),
                    ha='center', fontsize=9)

    ax.set_xlabel('Trading Days After Event', fontsize=12)
    ax.set_ylabel('Mean CAR (%)', fontsize=12)
    ax.set_title('Cumulative Abnormal Returns After Insider Cluster Events', fontsize=14)
    ax.set_xticks(days)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('paper/figures/car_curve.png', dpi=300)
    plt.close()


def plot_car_distribution(df):
    Path('paper/figures').mkdir(parents=True, exist_ok=True)

    clean = df['car_30'].dropna()
    mean_val = clean.mean()
    median_val = clean.median()

    lo = clean.quantile(0.005)
    hi = clean.quantile(0.995)
    display = clean[(clean >= lo) & (clean <= hi)]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(display * 100, bins=80, edgecolor='black', alpha=0.7, color='steelblue')
    ax.axvline(mean_val * 100, color='red', linestyle='--', linewidth=2,
               label=f'Mean: {mean_val:.2%}')
    ax.axvline(median_val * 100, color='green', linestyle='--', linewidth=2,
               label=f'Median: {median_val:.2%}')
    ax.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
    ax.set_xlabel('CAR(0,30) (%)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of 30-Day Cumulative Abnormal Returns', fontsize=14)
    ax.legend(fontsize=11)
    plt.tight_layout()
    plt.savefig('paper/figures/car_dist_30d.png', dpi=300)
    plt.close()


def plot_yearly_bar(df):
    Path('paper/figures').mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df['event_date'] = pd.to_datetime(df['event_date'])
    df['year'] = df['event_date'].dt.year

    yearly = df.groupby('year')['car_30'].agg(['mean', 'count', 'sem']).reset_index()
    yearly.columns = ['year', 'mean', 'count', 'sem']

    colors = ['steelblue' if m >= 0 else 'salmon' for m in yearly['mean']]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(yearly['year'], yearly['mean'] * 100, yerr=yearly['sem'] * 100 * 1.96,
                  capsize=4, color=colors, edgecolor='black', alpha=0.8)
    ax.axhline(0, color='red', linestyle='--', linewidth=1)

    for bar, count in zip(bars, yearly['count']):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f'n={count}', ha='center', va='bottom', fontsize=8)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Mean CAR(0,30) (%)', fontsize=12)
    ax.set_title('Mean 30-Day CAR by Year', fontsize=14)
    ax.set_xticks(yearly['year'])
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig('paper/figures/car_by_year.png', dpi=300)
    plt.close()


if __name__ == '__main__':
    df = load_event_returns()
    plot_car_curve(df)
    plot_car_distribution(df)
    plot_yearly_bar(df)

    expected_files = [
        'paper/figures/car_curve.png',
        'paper/figures/car_dist_30d.png',
        'paper/figures/car_by_year.png'
    ]

    for fp in expected_files:
        assert Path(fp).exists(), f'Missing: {fp}'
        assert Path(fp).stat().st_size > 10000, f'Too small: {fp}'

    n_car30 = df['car_30'].notna().sum()
    assert n_car30 > 2000