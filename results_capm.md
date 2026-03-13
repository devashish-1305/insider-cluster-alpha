# CAPM-adjusted robustness check

## Coverage

- Total events: 2664
- CAPM estimated (enough history): 2273 (85.3%)
- Skipped (insufficient history): 391 (14.7%)
- CAPM CAR(0,1) valid: 2273 (85.3%)
- CAPM CAR(0,5) valid: 2273 (85.3%)
- CAPM CAR(0,30) valid: 2235 (83.9%)
- CAPM CAR(0,60) valid: 2223 (83.4%)

## Summary statistics (t-test vs 0)

|    n |      mean |     median |       std |   t_stat |     p_value |   hit_rate |   ci_lower |   ci_upper | window    | significant   |
|-----:|----------:|-----------:|----------:|---------:|------------:|-----------:|-----------:|-----------:|:----------|:--------------|
| 2273 | 0.0129288 | 0.00669702 | 0.0593657 | 10.383   | 1.0479e-24  |   0.610647 |  0.0104882 |  0.0153694 | CAR(0,1)  | True          |
| 2273 | 0.0181745 | 0.00742175 | 0.0798597 | 10.8501  | 8.92117e-27 |   0.58073  |  0.0148914 |  0.0214576 | CAR(0,5)  | True          |
| 2235 | 0.0299121 | 0.020465   | 0.145237  |  9.73659 | 5.71525e-22 |   0.58613  |  0.0238907 |  0.0359335 | CAR(0,30) | True          |
| 2223 | 0.0574313 | 0.0404797  | 0.221947  | 12.2003  | 3.47999e-33 |   0.609537 |  0.0482048 |  0.0666577 | CAR(0,60) | True          |

## Interpretation

- The primary pre-registered test, CAR(0,30), shows a mean abnormal return of 2.99% (t=9.737, p=0.000000), which is statistically significant at the Bonferroni-adjusted threshold of 0.0125.
- The hit rate of 58.6% is above 50%, indicating that a majority of cluster events are followed by positive abnormal returns over 30 trading days.
- Significant abnormal returns are observed at: CAR(0,1), CAR(0,5), CAR(0,30), CAR(0,60).

## Market-adjusted vs CAPM-adjusted comparison

| window    |   market_adjusted_mean |   capm_adjusted_mean |   market_adjusted_n |   capm_adjusted_n |
|:----------|-----------------------:|---------------------:|--------------------:|------------------:|
| CAR(0,1)  |               0.01219  |             0.012929 |                2603 |              2273 |
| CAR(0,5)  |               0.01472  |             0.018175 |                2603 |              2273 |
| CAR(0,30) |               0.016521 |             0.029912 |                2565 |              2235 |
| CAR(0,60) |               0.032296 |             0.057431 |                2551 |              2223 |
