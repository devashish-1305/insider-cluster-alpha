# Insider Cluster Alpha Study

## Summary

This project tests whether clustered insider open-market purchases predict positive abnormal stock returns in U.S. equities. A cluster event is defined as 3 or more unique corporate insiders filing SEC Form 4 open-market purchases for the same stock within a rolling 30-calendar-day window. Using a fully pre-registered methodology applied to 2,664 cluster events across 1,030 firms from 2015 to 2024, the study finds a statistically significant mean 30-day cumulative abnormal return of +1.65% (t = 5.42, p = 6.6e-08). The result is robust to CAPM risk adjustment, alternative signal calibrations, and subsample splits by time period and market capitalization.

## Key Result

CAR(0, 30) = +1.65%, t = 5.42, p = 6.6e-08

## Repository Structure

```text
insider-cluster-alpha/
├── data/
│   ├── raw/                          # Raw insider filings, price data, market caps
│   └── processed/                    # Cleaned filings, detected events, event returns
├── notebooks/
│   ├── validate_clusters.py          # Cluster validation checks
│   └── validate_market_caps.py       # Market cap validation checks
├── paper/
│   ├── figures/                      # Generated figures (CAR curve, distribution, yearly)
│   ├── paper.md                      # Full research paper
│   └── insider_cluster_alpha.pdf     # PDF export
├── scrapers/
│   └── openinsider_scraper.py        # OpenInsider data collection
├── src/
│   ├── data_loader.py                # Data cleaning and standardization
│   ├── signal_definition.py          # Cluster detection algorithm
│   ├── event_study.py                # CAR computation (market-adjusted)
│   ├── event_filter.py               # Universe filters and overlap deduplication
│   ├── market_cap_collector.py       # Market cap data collection
│   ├── evaluation.py                 # Statistical tests and summary tables
│   ├── visualization.py              # Figures generation
│   ├── robustness_capm.py            # CAPM-adjusted robustness checks
│   ├── robustness_signals.py         # Signal variant sensitivity tests
│   └── robustness_subsamples.py      # Subsample stability tests
├── PREREGISTRATION.md                # Pre-registered methodology
├── requirements.txt                  # Python dependencies
├── results.md                        # Primary results output
├── results_capm.md                   # CAPM robustness results
├── results_robustness_signals.md     # Signal variant results
├── results_robustness_subsamples.md  # Subsample results
└── README.md                         # This file
```
## Reproduction

```bash
pip install -r requirements.txt
python src/evaluation.py
python src/visualization.py
```
Price data is downloaded via yfinance and may vary slightly depending on download date. Insider filing data is sourced from OpenInsider and stored in data/raw/.

Note: Do not run scripts that re-download data (e.g., data_loader / scrapers) unless you intend to refresh the dataset.

## Paper
See paper/paper.md for the full research paper or paper/insider_cluster_alpha.pdf for the formatted version.

## Pre-Registration
All primary methodological choices were locked in PREREGISTRATION.md before the full-sample analysis was conducted.