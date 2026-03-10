# Insider Cluster Alpha Study — Results

## Primary Statistical Tests

Bonferroni-adjusted significance threshold: p < 0.0125

| Window | N | Mean CAR | Median CAR | Std Dev | t-stat | p-value | Hit Rate | 95% CI | Significant |
|--------|---|----------|------------|---------|--------|---------|----------|--------|-------------|
| CAR(0,1) | 2603 | 0.0122 | 0.0065 | 0.0606 | 10.262 | 0.000000 | 0.606 | [0.0099, 0.0145] | Yes |
| CAR(0,5) | 2603 | 0.0147 | 0.0060 | 0.0816 | 9.202 | 0.000000 | 0.551 | [0.0116, 0.0179] | Yes |
| CAR(0,30) | 2565 | 0.0165 | 0.0045 | 0.1545 | 5.417 | 0.000000 | 0.518 | [0.0105, 0.0225] | Yes |
| CAR(0,60) | 2551 | 0.0323 | 0.0089 | 0.2697 | 6.047 | 0.000000 | 0.531 | [0.0218, 0.0428] | Yes |

## Interpretation

The primary pre-registered test, CAR(0,30), shows a mean abnormal return of 1.65% (t=5.417, p=0.000000), which is statistically significant at the Bonferroni-adjusted threshold of 0.0125.

The hit rate of 51.8% is above 50%, indicating that a majority of cluster events are followed by positive abnormal returns over 30 trading days.

Significant abnormal returns are observed at: CAR(0,1), CAR(0,5), CAR(0,30), CAR(0,60).
