cat > PREREGISTRATION.md << 'EOF'
Date: 2026-03-03

## Signal Definition
- Cluster Buy: 3+ unique insiders purchasing open-market shares
  of the same ticker within a rolling 30-calendar-day window
- Transaction type: Purchase only
- Event date: last FILING date in the cluster (NOT trade date)

## Universe Filters
- US equities only
- Minimum stock price: \$5
- Minimum market cap: \$300M
- Period: January 2015 to December 2024

## Overlap Rule
- Same ticker with multiple clusters within 60 calendar days:
  keep FIRST event only, drop subsequent

## Return Measurement
- CAR(0,1), CAR(0,5), CAR(0,30), CAR(0,60)
- CAR = stock cumulative return minus SPY cumulative return
- Returns computed from daily return series, date-aligned
- Cumulative return = product of (1 + daily returns) minus 1

## Statistical Tests
- Two-sided t-test of mean CAR against zero
- Significance level: 0.05
- Bonferroni-adjusted threshold: 0.0125 (4 tests)
- Primary horizon of interest: CAR(0,30)

## Abnormal Return Model
- Primary (Week 1): Market-adjusted (SPY subtraction)
- Secondary (Week 2): CAPM-adjusted

## Secondary Hypotheses (exploratory, only if primary fails)
1. Cluster buy + high short interest
2. Cluster buy weighted by dollar value
3. Alternative cluster sizes: 2+, 5+
4. Alternative windows: 14-day, 60-day

All secondary results labeled exploratory.
Multiple testing correction applied.
EOF