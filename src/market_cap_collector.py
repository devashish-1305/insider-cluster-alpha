import pandas as pd
import yfinance as yf
import time
import os
import logging


logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def load_tickers_from_prices(price_path: str) -> list:
    

    df = pd.read_csv(price_path, nrows=0)
    
    tickers = [col for col in df.columns if col != "Date"]
    print(f"[INFO] Found {len(tickers)} tickers in price_data.csv")
    return tickers


def load_existing_progress(output_path: str) -> dict:
    
    if os.path.exists(output_path):
        df = pd.read_csv(output_path)
        existing = dict(zip(df["ticker"], df["market_cap"]))
        print(f"[RESUME] Found {len(existing)} tickers already collected. Resuming...")
        return existing
    return {}


def fetch_market_cap(ticker_symbol: str) -> float:
    
    try:
        tk = yf.Ticker(ticker_symbol)
        info = tk.info
        
        # yfinance .info returns a dict; marketCap may or may not exist
        mktcap = info.get("marketCap", None)
        
        if mktcap is not None and mktcap > 0:
            return float(mktcap)
        
        ev = info.get("enterpriseValue", None)
        if ev is not None and ev > 0:
            return float(ev)
        
        return None
        
    except Exception as e:
        return None


def collect_market_caps(
    price_path: str = "data/raw/price_data.csv",
    output_path: str = "data/raw/market_caps.csv",
    sleep_between: float = 0.5,
    save_every: int = 50
):
    
    tickers = load_tickers_from_prices(price_path)
    collected = load_existing_progress(output_path)
    
    remaining = [t for t in tickers if t not in collected]
    print(f"[INFO] {len(remaining)} tickers remaining to process")
    print(f"[INFO] Estimated time: {len(remaining) * sleep_between / 60:.1f} minutes\n")
    
    failed = []
    new_count = 0
    
    for i, ticker in enumerate(remaining):
        mktcap = fetch_market_cap(ticker)
        
        if mktcap is not None:
            collected[ticker] = mktcap
            status = f"${mktcap / 1e9:.2f}B" if mktcap >= 1e9 else f"${mktcap / 1e6:.0f}M"
        else:
            collected[ticker] = None  # Record the failure too
            failed.append(ticker)
            status = "FAILED"
        
        new_count += 1
        
        # Progress print
        total_done = len(tickers) - len(remaining) + i + 1
        pct = total_done / len(tickers) * 100
        print(f"  [{total_done}/{len(tickers)}] ({pct:.1f}%) {ticker}: {status}")
        
        # Save incrementally
        if new_count % save_every == 0:
            _save_checkpoint(collected, output_path)
            print(f"  [SAVED] Checkpoint at {total_done} tickers\n")
        
        time.sleep(sleep_between)
    
    # Final save
    _save_checkpoint(collected, output_path)
    
    # Summary
    success = sum(1 for v in collected.values() if v is not None)
    fail = sum(1 for v in collected.values() if v is None)
    print(f"\n{'='*60}")
    print(f"[DONE] Market cap collection complete")
    print(f"  Total tickers:  {len(collected)}")
    print(f"  Success:        {success} ({success/len(collected)*100:.1f}%)")
    print(f"  Failed:         {fail} ({fail/len(collected)*100:.1f}%)")
    print(f"  Saved to:       {output_path}")
    print(f"{'='*60}")
    
    # Save failure 
    if failed:
        fail_df = pd.DataFrame({"ticker": failed})
        fail_path = "data/raw/failed_mktcap_tickers.csv"
        fail_df.to_csv(fail_path, index=False)
        print(f"  Failed tickers saved to: {fail_path}")
    
    return collected


def _save_checkpoint(collected: dict, output_path: str):
    df = pd.DataFrame([
        {"ticker": t, "market_cap": v} 
        for t, v in collected.items()
    ])
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    collect_market_caps()