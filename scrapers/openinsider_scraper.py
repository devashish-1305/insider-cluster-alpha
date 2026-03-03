import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os


COLUMNS = [
    'x', 'filing_date', 'trade_date', 'ticker', 'company_name',
    'insider_name', 'title', 'trade_type', 'price', 'qty',
    'owned', 'delta_own', 'value', '1d', '1w', '1m', '6m'
]


def build_url(date_from, date_to):
    """
    Build OpenInsider URL with correct encoding.
    date_from, date_to in MM/DD/YYYY format.
    """
    df_enc = date_from.replace('/', '%2F')
    dt_enc = date_to.replace('/', '%2F')

    url = (
        "http://openinsider.com/screener?"
        "s=&o=&pl=5&ph=&ll=&lh="
        "&fd=-1"
        f"&fdr={df_enc}+-+{dt_enc}"
        "&td=0&tdr="
        "&fdlyl=&fdlyh="
        "&dtefrom=&dteto="
        "&xp=1"
        "&vl=&vh=&ocl=&och="
        "&sic1=-1&sicl=100&sich=9999"
        "&grp=0"
        "&nfl=&nfh=&nil=&nih=&nol=&noh="
        "&v2l=&v2h=&oc2l=&oc2h="
        "&sortcol=1&cnt=5000&page=1"
    )
    return url


def parse_table(html):
    """
    Parse insider data table using BeautifulSoup.
    Returns clean dataframe.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Find the main data table
    table = soup.find('table', class_='tinytable')

    if table is None:
        # Fallback: find the table with most rows
        all_tables = soup.find_all('table')
        if len(all_tables) == 0:
            return pd.DataFrame()
        table = max(all_tables, key=lambda t: len(t.find_all('tr')))

    # Get all data rows (skip header)
    tbody = table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        rows = table.find_all('tr')[1:]  # skip header row

    if len(rows) == 0:
        return pd.DataFrame()

    # Extract text from each cell in each row
    data = []
    for row in rows:
        cells = row.find_all('td')
        row_data = []
        for cell in cells:
            text = cell.get_text(strip=True)
            row_data.append(text)

        # Only keep rows with enough columns (skip junk rows)
        if len(row_data) >= 13:
            data.append(row_data)

    if len(data) == 0:
        return pd.DataFrame()

    # Trim or pad each row to match expected columns
    cleaned_data = []
    for row in data:
        if len(row) >= len(COLUMNS):
            cleaned_data.append(row[:len(COLUMNS)])
        else:
            padded = row + [''] * (len(COLUMNS) - len(row))
            cleaned_data.append(padded)

    df = pd.DataFrame(cleaned_data, columns=COLUMNS)
    return df


def clean_numeric(series):
    """
    '\$1,502,500' → 1502500.0
    '+10,000'    → 10000.0
    '+12%'       → 12.0
    junk         → NaN
    """
    cleaned = (
        series
        .astype(str)
        .str.replace('$', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.replace('+', '', regex=False)
        .str.replace('%', '', regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors='coerce')


def clean_dataframe(df):
    """Clean all numeric columns."""
    for col in ['price', 'qty', 'owned', 'value']:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    for col in ['delta_own', '1d', '1w', '1m', '6m']:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    return df


def scrape_period(date_from, date_to):
    """Fetch one date range from OpenInsider."""
    url = build_url(date_from, date_to)

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )
    }

    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code != 200:
        return pd.DataFrame(), f"HTTP {resp.status_code}"

    df = parse_table(resp.text)

    if len(df) == 0:
        return pd.DataFrame(), "No rows parsed from HTML"

    return df, "ok"


def main():
    os.makedirs("data/raw", exist_ok=True)

    # Every half-year from 2015 to 2024
    periods = []
    for year in range(2015, 2025):
        periods.append((f"01/01/{year}", f"06/30/{year}", f"oi_{year}_h1"))
        periods.append((f"07/01/{year}", f"12/31/{year}", f"oi_{year}_h2"))

    all_dfs = []
    failed = []

    print(f"Scraping {len(periods)} periods from OpenInsider\n")

    for date_from, date_to, name in periods:
        print(f"  {name:15s} ({date_from} - {date_to})  ", end="", flush=True)

        try:
            df, status = scrape_period(date_from, date_to)

            if status != "ok":
                print(f"→ FAILED: {status}")
                failed.append(name)
            elif len(df) == 0:
                print("→ 0 rows")
                failed.append(name)
            else:
                # Quick check: verify dates are in expected range
                sample_dates = df['filing_date'].head(3).tolist()
                print(f"→ {len(df)} rows  (sample: {sample_dates[0]})")

                path = f"data/raw/{name}.csv"
                df.to_csv(path, index=False)
                all_dfs.append(df)

        except Exception as e:
            print(f"→ ERROR: {e}")
            failed.append(name)

        time.sleep(3)

    # Combine
    if len(all_dfs) == 0:
        print("\nFATAL: No data collected.")
        if len(failed) > 0:
            print(f"All {len(failed)} periods failed.")
            print("\nDEBUG: Testing one URL manually...")
            test_url = build_url("01/01/2023", "06/30/2023")
            print(f"URL: {test_url}")
            print("Open this URL in your browser. Do you see data?")
        return

    combined = pd.concat(all_dfs, ignore_index=True)

    # Clean numeric columns
    combined = clean_dataframe(combined)

    # Save
    combined.to_csv("data/raw/insider_filings.csv", index=False)

    # Report
    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"{'='*60}")
    print(f"  Periods scraped:  {len(all_dfs)} / {len(periods)}")
    print(f"  Total rows:       {len(combined)}")

    print(f"\n  Columns and types:")
    for col in combined.columns:
        dtype = combined[col].dtype
        nulls = combined[col].isnull().sum()
        sample = combined[col].dropna().iloc[0] if combined[col].dropna().shape[0] > 0 else 'EMPTY'
        print(f"    {col:15s}  {str(dtype):10s}  nulls={nulls:5d}  sample={sample}")

    # Verify previously broken columns
    print(f"\n  Numeric verification:")
    for col in ['price', 'qty', 'value']:
        data = combined[col].dropna()
        if len(data) > 0 and data.dtype in ['float64', 'int64']:
            print(f"    {col:10s}  ✓  min={data.min():.2f}  max={data.max():.2f}  count={len(data)}")
        else:
            print(f"    {col:10s}  ✗  NOT NUMERIC — needs investigation")

    if len(failed) > 0:
        print(f"\n  Failed periods ({len(failed)}):")
        for f in failed:
            print(f"    {f}")

    print(f"\n  Saved: data/raw/insider_filings.csv")


if __name__ == "__main__":
    main()