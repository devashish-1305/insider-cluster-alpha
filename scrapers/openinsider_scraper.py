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
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', class_='tinytable')

    if table is None:
        all_tables = soup.find_all('table')
        if len(all_tables) == 0:
            return pd.DataFrame()
        table = max(all_tables, key=lambda t: len(t.find_all('tr')))

    tbody = table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        rows = table.find_all('tr')[1:]

    if len(rows) == 0:
        return pd.DataFrame()

    data = []
    for row in rows:
        cells = row.find_all('td')
        row_data = []
        for cell in cells:
            text = cell.get_text(strip=True)
            row_data.append(text)

        if len(row_data) >= 13:
            data.append(row_data)

    if len(data) == 0:
        return pd.DataFrame()

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
    for col in ['price', 'qty', 'owned', 'value']:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    for col in ['delta_own', '1d', '1w', '1m', '6m']:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    return df


def scrape_period(date_from, date_to):
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

    periods = []
    for year in range(2015, 2025):
        periods.append((f"01/01/{year}", f"06/30/{year}", f"oi_{year}_h1"))
        periods.append((f"07/01/{year}", f"12/31/{year}", f"oi_{year}_h2"))

    all_dfs = []
    failed = []

    for date_from, date_to, name in periods:
        try:
            df, status = scrape_period(date_from, date_to)

            if status != "ok":
                failed.append(name)
            elif len(df) == 0:
                failed.append(name)
            else:
                path = f"data/raw/{name}.csv"
                df.to_csv(path, index=False)
                all_dfs.append(df)

        except Exception as e:
            failed.append(name)

        time.sleep(3)

    if len(all_dfs) == 0:
        return

    combined = pd.concat(all_dfs, ignore_index=True)

    combined = clean_dataframe(combined)

    combined.to_csv("data/raw/insider_filings.csv", index=False)


if __name__ == "__main__":
    main()