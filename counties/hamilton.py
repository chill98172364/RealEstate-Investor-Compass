import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

BASE_URL = "https://wedge1.hcauditor.org"

def fetch_sales(start_date, end_date):
    print(f"[DEBUG] Hamilton County: Fetching sales from {start_date} to {end_date}")

    session = requests.Session()

    # Post search criteria for sales
    payload = {
        "search_type": "Sales",
        "sale_book": "",
        "sale_page": "",
        "sale_price_low": "",
        "sale_price_high": "",
        "sale_date_low": start_date,
        "sale_date_high": end_date,
        "year_built_low": "",
        "year_built_high": "",
        "finished_sq_ft_low": "",
        "finished_sq_ft_high": "",
        "acreage_low": "",
        "acreage_high": "",
        "bedrooms_low": "",
        "bedrooms_high": "",
        "fireplaces_low": "",
        "fireplaces_high": "",
        "full_baths_low": "",
        "full_baths_high": "",
        "half_baths_low": "",
        "half_baths_high": "",
        "total_rooms_low": "",
        "total_rooms_high": "",
        "stories_low": "",
        "stories_high": "",
        "origin_property_key": "",
        "feet_from_origin": ""
    }

    # Post search request
    post_url = f"{BASE_URL}/execute"
    print(f"[DEBUG] Posting sales search request to {post_url}")
    resp = session.post(post_url, data=payload)
    resp.raise_for_status()

    # Get the printable results page
    print("[DEBUG] Fetching printable sales results page...")
    r = session.get(f"{BASE_URL}/results/print")
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr")

    data = []
    print(f"[DEBUG] Found {len(rows)} sales table rows to process")

    for tr in rows:
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]

        # Skip header or junk lines
        if (
            not cells
            or "Parcel" in cells[0]
            or "Searched for" in cells[0]
            or cells[0].strip() == ""
        ):
            continue

        # Sales history format - only keep if it has a sale price
        if len(cells) == 6:
            parcel, owner, address, roll, sale_date, sale_price = cells
            # Only include if it has a sale price (meaning it actually sold)
            if sale_price.strip():
                data.append({
                    "parcel_id": parcel,
                    "address": address,
                    "fin_sqft": "",
                    "year_built": "",
                    "sale_date": sale_date,
                    "sale_price": sale_price
                })

        # Property details format - only keep if it has a transfer date and amount
        elif len(cells) == 7:
            parcel, address, bbb, fin_sqft, use, year_built, transfer_date = cells
            # Skip if no transfer date or it's not a recent sale
            if transfer_date.strip():
                data.append({
                    "parcel_id": parcel,
                    "address": address,
                    "fin_sqft": fin_sqft,
                    "year_built": year_built,
                    "sale_date": transfer_date,
                    "sale_price": ""
                })

        # Property details + amount format - only keep if it has both date and amount
        elif len(cells) == 8:
            parcel, address, bbb, fin_sqft, use, year_built, transfer_date, amount = cells
            # Only include if it has both a transfer date and sale amount
            if transfer_date.strip() and amount.strip():
                data.append({
                    "parcel_id": parcel,
                    "address": address,
                    "fin_sqft": fin_sqft,
                    "year_built": year_built,
                    "sale_date": transfer_date,
                    "sale_price": amount
                })

    return clean_sold_data(data)

def clean_sold_data(data):
    """Clean and process sold property data"""
    df = pd.DataFrame(data)
    print(f"[DEBUG] Parsed {len(df)} raw sold rows for Hamilton County")

    if df.empty:
        print("[DEBUG] No sold data found for Hamilton County")
        return df

    # Remove exact duplicates
    df.drop_duplicates(inplace=True)
    print(f"[DEBUG] After removing duplicates: {len(df)} sold rows")

    # Convert sale_date to datetime and filter by date range
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = df.dropna(subset=["sale_date"])
    
    # Keep only the latest sale per property (by parcel_id)
    df = df.sort_values("sale_date", ascending=False)
    df = df.drop_duplicates(subset=["parcel_id"], keep="first")
    print(f"[DEBUG] After keeping latest sale per property: {len(df)} sold rows")

    # Clean up sale_price - remove $ and commas, convert to numeric
    df["sale_price"] = df["sale_price"].astype(str).str.replace("$", "").str.replace(",", "")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    
    # Remove rows with no sale price or very low prices (likely errors)
    df = df.dropna(subset=["sale_price"])
    df = df[df["sale_price"] > 1000]  # Filter out very low prices that are likely errors
    print(f"[DEBUG] After price filtering: {len(df)} sold rows")

    # Format sale_date back to string for consistency
    df["sale_date"] = df["sale_date"].dt.strftime("%Y-%m-%d")
    
    # Format sale_price back to currency string
    df["sale_price"] = df["sale_price"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")

    # Keep consistent column order
    df = df[["parcel_id", "address", "fin_sqft", "year_built", "sale_date", "sale_price"]]

    print(f"[DEBUG] Final Hamilton County sold results: {len(df)} clean records")
    return df
