import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

BASE_URL = "https://propertysearch.bcohio.gov/search/advancedsearch.aspx?mode=sales"
PRINTABLE_URL = "https://propertysearch.bcohio.gov/Search/PrintSearch.aspx?type=SALES&sIndex=0"

def get_all_hidden_fields(soup):
    hidden = {}
    for el in soup.find_all("input", type="hidden"):
        if el.get("name") and el.get("value") is not None:
            hidden[el["name"]] = el["value"]
    return hidden

def fetch_sales(start_date, end_date):
    print(f"[DEBUG] Butler County: Fetching sales from {start_date} to {end_date}")
    
    session = requests.Session()

    # Step 1 — Load search page
    r = session.get(BASE_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = get_all_hidden_fields(soup)

    # Step 2 — Submit search
    payload = dict(hidden)
    payload.update({
        "SaleSearchOptions$SaleDateFrom": start_date,
        "SaleSearchOptions$SaleDateTo": end_date,
        "SaleSearchOptions$chkSaleDate": "on",
        "cmdSearch": "Search"
    })
    session.post(BASE_URL, data=payload).raise_for_status()

    # Step 3 — Printable results page
    r = session.get(PRINTABLE_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Step 4 — Parse table
    rows = soup.find_all("tr")
    results = []
    print(f"[DEBUG] Found {len(rows)} table rows to process")
    
    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        if (
            not cells
            or "Parcel" in cells[0]
            or "Searched for" in cells[0]
            or cells[0].strip() == ""
        ):
            continue
        if len(cells) >= 6:
            # Only include if it has a sale price (meaning it actually sold)
            if cells[5].strip():
                results.append({
                    'parcel_id': cells[0],
                    'address': cells[2],
                    'fin_sqft': '',
                    'year_built': '',
                    'sale_date': cells[4],
                    'sale_price': cells[5]
                })

    # Step 5: Create DataFrame and clean data
    df = pd.DataFrame(results)
    print(f"[DEBUG] Parsed {len(df)} raw rows for Butler County")

    if df.empty:
        print("[DEBUG] No data found for Butler County")
        return df

    # Remove exact duplicates
    df.drop_duplicates(inplace=True)
    print(f"[DEBUG] After removing duplicates: {len(df)} rows")

    # Convert sale_date to datetime and filter by date range
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df.dropna(subset=["sale_date"])
    
    # Convert start_date and end_date to datetime for filtering
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Filter to only include sales within the specified date range - Redundant
    df = df[(df["sale_date"] >= start_dt) & (df["sale_date"] <= end_dt)]
    print(f"[DEBUG] After date filtering: {len(df)} rows")

    # Keep only the latest sale per property (by parcel_id)
    df = df.sort_values("sale_date", ascending=False)
    df = df.drop_duplicates(subset=["parcel_id"], keep="first")
    print(f"[DEBUG] After keeping latest sale per property: {len(df)} rows")

    # Clean up sale_price - remove $ and commas, convert to numeric
    df["sale_price"] = df["sale_price"].astype(str).str.replace("$", "").str.replace(",", "")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    
    # Remove rows with no sale price or very low prices (likely errors)
    df = df.dropna(subset=["sale_price"])
    df = df[df["sale_price"] > 1000]  # Filter out very low prices that are likely errors
    print(f"[DEBUG] After price filtering: {len(df)} rows")

    # Format sale_date back to string for consistency
    df["sale_date"] = df["sale_date"].dt.strftime("%Y-%m-%d")
    
    # Format sale_price back to currency string
    df["sale_price"] = df["sale_price"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")

    # Keep consistent column order
    df = df[["parcel_id", "address", "fin_sqft", "year_built", "sale_date", "sale_price"]]

    print(f"[DEBUG] Final Butler County results: {len(df)} clean records")
    return df
