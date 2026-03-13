# CAPM-adjusted robustness check

## Summary statistics (t-test vs 0)

|    n |      mean |     median |       std |   t_stat |     p_value |   hit_rate |   ci_lower |   ci_upper | window    | significant   |
|-----:|----------:|-----------:|----------:|---------:|------------:|-----------:|-----------:|-----------:|:----------|:--------------|
| 2225 | 0.0127637 | 0.00655509 | 0.0589525 |  10.2127 | 5.82725e-24 |   0.608989 |  0.0103141 |  0.0152133 | CAR(0,1)  | True          |
| 2225 | 0.0181055 | 0.00743805 | 0.0794706 |  10.7466 | 2.68691e-26 |   0.582022 |  0.0148034 |  0.0214077 | CAR(0,5)  | True          |
| 2187 | 0.0311199 | 0.0212535  | 0.144351  |  10.0819 | 2.13752e-23 |   0.590306 |  0.0250699 |  0.0371698 | CAR(0,30) | True          |
| 2175 | 0.0584358 | 0.04096    | 0.220669  |  12.35   | 6.49405e-34 |   0.611954 |  0.0491618 |  0.0677098 | CAR(0,60) | True          |

## Interpretation

- The primary pre-registered test, CAR(0,30), shows a mean abnormal return of 3.11% (t=10.082, p=0.000000), which is statistically significant at the Bonferroni-adjusted threshold of 0.0125.
- The hit rate of 59.0% is above 50%, indicating that a majority of cluster events are followed by positive abnormal returns over 30 trading days.
- Significant abnormal returns are observed at: CAR(0,1), CAR(0,5), CAR(0,30), CAR(0,60).

## Market-adjusted vs CAPM-adjusted (means)

| window    |   market_adjusted_mean |   capm_adjusted_mean |
|:----------|-----------------------:|---------------------:|
| CAR(0,30) |              0.0165212 |            0.0311199 |
