import os
import importlib
import pandas as pd
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from datetime import datetime, timedelta
from make_graph import Make_graph
from json import loads
import os

from dotenv import load_dotenv; load_dotenv();

# ===== CONFIG ===== #
COUNTIES_FOLDER = "counties"
OUTPUT_FOLDER = "output"

INVESTOR_MSG = """
Attached is the latest real estate sales report from all monitored counties.
Data has been cleaned and filtered for investor use, showing only recent property sales with confirmed prices.

Collumn explanations:
parcel_id - The unique property ID assigned by the county auditor.
fin_sqft - Finished square footage of the property. Some sites dont return this info so it might be blank for certain counties.
price_per_sqft - Sale price divided by finished square feet. Useful for comparing across different sized homes.
est_monthly_rent - A rent estimate using the “1% rule” (≈ 1% of sale price per month).
est_roi - Estimated return on investment (cap rate style) based on rent vs sale price.
cash_sale_flag - “True” if the property appears to have sold for cash (sale price = 0 or unusual entry). Potential errors as this is simply a guess.
deal_flag - “True” if this property sold at least 20% below the median price per square foot for the county (potential below-market deal).
"""

# === EMAIL SETTINGS === #
settingsJson = loads(open("settings.json", "r").read())
SMTP_SERVER = settingsJson["SMTP"]["Server"]
SMTP_PORT = settingsJson["SMTP"]["Port"]
SMTP_USER = settingsJson["SMTP"]["User"]
SMTP_PASS = os.getenv('SMTP_PASS')
EMAIL_FROM = settingsJson["Email"]["From"]
EMAIL_RECIPIENTS = settingsJson["Email"]["Recipients"]
print("[DEBUG] Loaded settings from 'settings.json'")

# ===== HELPER FUNCTIONS ===== #
def send_email_with_attachment(subject, body, files):
    print("\n[DEBUG] Preparing email with attachments...")
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["Bcc"] = ", ".join(EMAIL_RECIPIENTS)
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    for file_path in files:
        print(f"[DEBUG] Attaching file: {file_path}")
        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(file_path)}",
        )
        msg.attach(part)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        print("[DEBUG] Connecting to SMTP server...")
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(
            SMTP_USER,
            EMAIL_RECIPIENTS,
            msg.as_string(),
        )
    print("[DEBUG] Email sent successfully.")

def enhance_data(df: pd.DataFrame):
    # De-dupe
    df = df.drop_duplicates(subset=['parcel_id'], keep='first')

    # Clean price
    df["sale_price_clean"] = (
        df["sale_price"]
        .replace("[\\$,]", "", regex=True)
        .astype(float)
        .fillna(0)
    )

    # Price per sqft
    if "fin_sqft" in df.columns:
        df["fin_sqft"] = pd.to_numeric(df["fin_sqft"], errors="coerce").fillna(0).astype(int)
        df["price_per_sqft"] = df.apply(
            lambda row: row["sale_price_clean"] / row["fin_sqft"]
            if row["fin_sqft"] > 0 else None,
            axis=1
        )
    else:
        df["price_per_sqft"] = None

    # Rent estimate (1% rule)
    df["est_monthly_rent"] = (df["sale_price_clean"] * 0.01).round(0)

    # ROI (cap rate style)
    df["est_roi"] = df.apply(
        lambda row: (row["est_monthly_rent"] * 12 / row["sale_price_clean"])
        if row["sale_price_clean"] > 0 else None,
        axis=1
    )

    # Flag cash/odd sales
    df["cash_sale_flag"] = df["sale_price_clean"] == 0

    # --- Outlier filtering applied globally (not just for summaries) ---
    q1, q3 = df["sale_price_clean"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    df = df[(df["sale_price_clean"] >= lower) & (df["sale_price_clean"] <= upper)]

    if df["price_per_sqft"].notna().sum() > 0:
        q1_ppsf, q3_ppsf = df["price_per_sqft"].quantile([0.25, 0.75])
        iqr_ppsf = q3_ppsf - q1_ppsf
        lower_ppsf, upper_ppsf = q1_ppsf - 1.5 * iqr_ppsf, q3_ppsf + 1.5 * iqr_ppsf
        df = df[
            (df["price_per_sqft"].isna()) |
            ((df["price_per_sqft"] >= lower_ppsf) & (df["price_per_sqft"] <= upper_ppsf))
        ]

    # Median PPSF for deal flagging (on cleaned data)
    median_ppsf = df["price_per_sqft"].median(skipna=True)
    df["deal_flag"] = df["price_per_sqft"] < (0.8 * median_ppsf)

    # Sort by price ascending
    df_sorted = df.sort_values("sale_price_clean", ascending=True)

    # --- Build summaries per county (already filtered) ---
    county_summaries = []
    for county_name, g in df_sorted.groupby("county"):
        county_summary = {
            "median_sale_price": g["sale_price_clean"].median(),
            "median_price_per_sqft": g["price_per_sqft"].median(skipna=True),
            "total_records": len(g),
            "flagged_deals": g["deal_flag"].sum(),
            "cash_sales": g["cash_sale_flag"].sum(),
        }
        summary_text = (
            f"{county_name.title()} County:\n"
            f"- Median Sale Price: ${county_summary['median_sale_price']:.0f}\n"
            f"- Median Price per SqFt: ${county_summary['median_price_per_sqft']:.0f}\n"
            f"- Total Records: {county_summary['total_records']}\n"
            f"- Flagged Deals: {county_summary['flagged_deals']}\n"
            f"- Cash Sales: {county_summary['cash_sales']}\n"
        )
        county_summaries.append(summary_text)

    # Join all summaries into one string
    full_summary = "\n".join(county_summaries)

    return df_sorted, full_summary

# ===== MAIN SCRIPT =====
if __name__ == "__main__":
    print("[DEBUG] Starting multi-county sales fetch...")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Default date range: last 120 days (more reasonable for recent sales)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=120)
    start_str = start_date.strftime("%m/%d/%Y")
    end_str = end_date.strftime("%m/%d/%Y")
    print(f"[DEBUG] Date range: {start_str} to {end_str}")

    all_sold_dfs = []
    output_files = []
    summary = ""

    for file in os.listdir(COUNTIES_FOLDER):
        if file.endswith(".py") and not file.startswith("__"):
            county_name = file[:-3]
            print(f"[DEBUG] Processing county module: {county_name}")
            county = importlib.import_module(f"{COUNTIES_FOLDER}.{county_name}")

            if not hasattr(county, "fetch_sales"):
                print(f"[DEBUG] Skipping {county_name} (no fetch_sales function)")
                continue

            try:
                # Fetch sold properties
                sold_df = county.fetch_sales(start_str, end_str)
                
                # Process sold properties
                if isinstance(sold_df, pd.DataFrame) and not sold_df.empty:
                    sold_df['county'] = county_name.title()
                    all_sold_dfs.append(sold_df)
                    print(f"[DEBUG] {county_name} sold properties: {len(sold_df)} records")

            except Exception as e:
                print(f"[DEBUG] Error processing {county_name}: {e}")

    # Combine all sold properties into one CSV
    if all_sold_dfs:
        combined_sold_df = pd.concat(all_sold_dfs, ignore_index=True)
        
        # Final cleaning for sold properties
        final_df, summary = enhance_data(combined_sold_df)
        INVESTOR_MSG += "\nIndividual County Summaries:\n"+summary
        sold_file = os.path.join(OUTPUT_FOLDER, f"ALL_SOLD_{start_str.replace('/','-')}_to_{end_str.replace('/','-')}.csv")
        final_df.to_csv(sold_file, index=False)
        print(f"[DEBUG] Saved {len(final_df)} sold records to {sold_file}")
        output_files.append(sold_file)
        print("[DEBUG] Making graph...")
        Make_graph(sold_file)
    else:
        print("[DEBUG] No sold properties found")

    # Send email with combined files
    if output_files:
        print(f"[DEBUG] Would have sent email:\n{INVESTOR_MSG}")
        # print("[DEBUG] Sending email with attachments...")
        # send_email_with_attachment(
        #     subject=f"Real Estate Sales Report - {start_str} to {end_str}",
        #     body=INVESTOR_MSG,
        #     files=output_files
        # )
        # print(f"[DEBUG] Email sent with {len(output_files)} attachments")
    else:
        print("[DEBUG] No files to send, no email sent.")