import pandas as pd
import numpy as np
import yfinance as yf
import time
import logging
from pathlib import Path

#suppress yf spam 
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

# paths
ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)


def clean_insider_data():
    """Load raw insider CSV, clean it, return dataframe."""
    path = RAW / "insider_filings.csv"
    df = pd.read_csv(path)
    log.info(f"Raw rows: {len(df):,}")
    log.info(f"Raw columns: {list(df.columns)}")

    # standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    renames = {"1d": "ret_1d", "1w": "ret_1w", "1m": "ret_1m", "6m": "ret_6m"}
    df.rename(columns={k: v for k, v in renames.items() if k in df.columns}, inplace=True)
    if "x" in df.columns:
        df.rename(columns={"x": "oi_index"}, inplace=True)

    # clean tickers
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"].notna() & (df["ticker"] != "") & (df["ticker"] != "NAN")]

    # removes invalid tickers: only allows letters, dots, hyphens, 1-6 chars
    before = len(df)
    df = df[df["ticker"].str.match(r"^[A-Z][A-Z\.\-]{0,5}$", na=False)]
    df = df[~df["ticker"].isin(["0", "NONE", "NULL", "NA", "NAN"])]
    log.info(f"Ticker cleaning: {before:,} -> {len(df):,} ({before - len(df):,} invalid removed)")

    # parse dates
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    before = len(df)
    df.dropna(subset=["filing_date", "trade_date"], inplace=True)
    log.info(f"Date parsing: dropped {before - len(df):,} rows with bad dates")

    # filter purchases only
    log.info(f"Trade types:\n{df['trade_type'].value_counts().head(10)}")
    mask = df["trade_type"].str.strip().str.lower() == "p - purchase"
    if mask.sum() < 100:
        mask = df["trade_type"].str.strip().str.lower().str.startswith("p - purchase")
    df = df[mask].copy()
    log.info(f"After purchase filter: {len(df):,}")

    # price >= \$5
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    before = len(df)
    df = df[df["price"] >= 5.0].copy()
    log.info(f"After price filter (>=\$5): {len(df):,} ({before - len(df):,} removed)")

    # deduplicate
    before = len(df)
    df.drop_duplicates(subset=["ticker", "trade_date", "insider_name"], keep="first", inplace=True)
    log.info(f"Dedup: {before:,} -> {len(df):,} ({before - len(df):,} duplicates)")

    df.sort_values(["filing_date", "ticker"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    log.info(f"\n{'='*50}")
    log.info(f"CLEAN DATA SUMMARY")
    log.info(f"  Rows:           {len(df):,}")
    log.info(f"  Unique tickers: {df['ticker'].nunique():,}")
    log.info(f"  Unique insiders:{df['insider_name'].nunique():,}")
    log.info(f"  Trade dates:    {df['trade_date'].min().date()} to {df['trade_date'].max().date()}")
    log.info(f"  Filing dates:   {df['filing_date'].min().date()} to {df['filing_date'].max().date()}")
    log.info(f"{'='*50}\n")

    return df


def download_prices(tickers, start="2014-06-01", end="2025-01-01", batch_size=50):
    """Download adjusted close prices for list of tickers. Returns wide dataframe."""
    frames = []
    failed = []
    total = (len(tickers) + batch_size - 1) // batch_size

    log.info(f"Downloading prices: {len(tickers)} tickers, {total} batches")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        batch_num = i // batch_size + 1

        try:
            data = yf.download(
                batch, start=start, end=end,
                auto_adjust=True, progress=False, threads=True,
            )

            if data.empty:
                log.warning(f"  Batch {batch_num}/{total}: empty result, skipping")
                failed.extend(batch)
                continue

            # extract close prices
            if isinstance(data.columns, pd.MultiIndex):
                prices = data["Close"]
            else:
                # single ticker
                prices = data[["Close"]].rename(columns={"Close": batch[0]})

            # count how many tickers actually came back
            if isinstance(prices, pd.Series):
                got = 1
            else:
                got = prices.shape[1]
                # track which tickers in batch got no data
                for t in batch:
                    if t not in prices.columns:
                        failed.append(t)
                    elif prices[t].dropna().empty:
                        failed.append(t)
                        prices.drop(columns=[t], inplace=True)
                        got -= 1

            frames.append(prices)
            log.info(f"  Batch {batch_num}/{total}: got {got}/{len(batch)} tickers")

        except KeyboardInterrupt:
            log.info("\nDownload stopped by user (Ctrl+C)")
            log.info(f"  Completed {batch_num - 1}/{total} batches before stopping")
            break

        except Exception as e:
            log.error(f"  Batch {batch_num}/{total} FAILED: {type(e).__name__}: {e}")
            failed.extend(batch)

        if batch_num < total:
            time.sleep(1.5)

    if not frames:
        log.error("No price data downloaded at all!")
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1)
    combined = combined.loc[:, ~combined.columns.duplicated()]
    combined.index = pd.to_datetime(combined.index)
    combined.index.name = "date"

    # deduplicate failed list
    failed = sorted(set(failed))

    # save failed tickers for transparency
    if failed:
        failed_df = pd.DataFrame({"ticker": failed})
        failed_df.to_csv(RAW / "failed_price_tickers.csv", index=False)
        log.info(f"Saved failed tickers list: {len(failed)} tickers -> failed_price_tickers.csv")

    success_rate = combined.shape[1] / len(tickers) * 100
    log.info(f"\nPrice download complete:")
    log.info(f"  Requested:  {len(tickers)}")
    log.info(f"  Downloaded: {combined.shape[1]}")
    log.info(f"  Failed:     {len(failed)}")
    log.info(f"  Success:    {success_rate:.1f}%")
    log.info(f"  Days:       {combined.shape[0]}")

    if success_rate < 50:
        log.warning("WARNING: Less than 50% success rate. Check your ticker list.")

    return combined


def download_spy(start="2014-06-01", end="2025-01-01"):
    """Download SPY adjusted close prices."""
    log.info("Downloading SPY...")
    data = yf.download("SPY", start=start, end=end, auto_adjust=True, progress=False)

    if isinstance(data.columns, pd.MultiIndex):
        spy = data["Close"]
        if isinstance(spy, pd.DataFrame):
            spy = spy.iloc[:, 0]
    else:
        spy = data["Close"]

    spy = spy.to_frame("SPY")
    spy.index = pd.to_datetime(spy.index)
    spy.index.name = "date"
    log.info(f"SPY: {len(spy)} days, {spy.index[0].date()} to {spy.index[-1].date()}")
    return spy


def spot_check(prices, n=3):
    """Print stats for n random tickers as sanity check."""
    cols = [c for c in prices.columns if prices[c].dropna().shape[0] > 0]
    sample = np.random.choice(cols, size=min(n, len(cols)), replace=False)
    log.info(f"\nSpot check ({n} random tickers):")
    for t in sample:
        s = prices[t].dropna()
        log.info(f"  {t}: {len(s)} days, ${s.min():.2f}-${s.max():.2f}, latest ${s.iloc[-1]:.2f}")


def main():
    # step 1: clean insider data
    df = clean_insider_data()
    out = PROCESSED / "insider_cleaned.csv"
    df.to_csv(out, index=False)
    log.info(f"Saved {out}")

    # step 2: download stock prices
    tickers = sorted(df["ticker"].unique().tolist())
    log.info(f"\nStarting price download for {len(tickers)} tickers...")
    log.info(f"(This will take 15-30 minutes. Ctrl+C to stop early if needed.)\n")

    prices = download_prices(tickers)

    if not prices.empty:
        prices.to_csv(RAW / "price_data.csv")
        log.info(f"Saved {RAW / 'price_data.csv'}")
        spot_check(prices)
    else:
        log.error("No price data to save!")
        return

    # step 3: download SPY
    spy = download_spy()
    spy.to_csv(RAW / "spy_prices.csv")
    log.info(f"Saved {RAW / 'spy_prices.csv'}")

    # summary
    log.info(f"\n{'='*50}")
    log.info(f"DAY 2 COMPLETE")
    log.info(f"  Cleaned filings: {len(df):,} rows, {df['ticker'].nunique():,} tickers")
    log.info(f"  Price matrix:    {prices.shape[0]} days x {prices.shape[1]} tickers")
    log.info(f"  SPY:             {len(spy)} days")
    log.info(f"{'='*50}")


if __name__ == "__main__":
    main()