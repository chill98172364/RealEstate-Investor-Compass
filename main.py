import os
import importlib
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from make_graph import Make_graph

BASE_URL = "https://propertysearch.bcohio.gov/search/advancedsearch.aspx?mode=sales"
PRINTABLE_URL = "https://propertysearch.bcohio.gov/Search/PrintSearch.aspx?type=SALES&sIndex=0"

# ===== CONFIG ===== #
COUNTIES_FOLDER = "counties"
OUTPUT_FOLDER = "output"

# === Email Settings === #
EMAIL_FROM = "clownaltt@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "clownaltt@gmail.com"
SMTP_PASS =                                                                                                                                                                                                                                                          "lujv qvsn nssc mvdv"
EMAIL_RECIPIENTS = [
    "cohizalalt3@gmail.com",
    "cohizal@gmail.com"
]

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
        combined_sold_df = combined_sold_df.drop_duplicates(subset=['parcel_id'], keep='first')
        combined_sold_df = combined_sold_df.sort_values('sale_date', ascending=False)
        
        sold_file = os.path.join(OUTPUT_FOLDER, f"ALL_SOLD_{start_str.replace('/','-')}_to_{end_str.replace('/','-')}.csv")
        combined_sold_df.to_csv(sold_file, index=False)
        print(f"[DEBUG] Saved {len(combined_sold_df)} sold records to {sold_file}")
        output_files.append(sold_file)
        print("[DEBUG] Making graph...")
        Make_graph(sold_file)
    else:
        print("[DEBUG] No sold properties found")

    # Send email with combined files
    if output_files:
        print("[DEBUG] Would have sent email")
        # print("[DEBUG] Sending email with attachments...")
        # send_email_with_attachment(
        #     subject=f"Real Estate Sales Report - {start_str} to {end_str}",
        #     body="Attached is the latest real estate sales report from all monitored counties.\n\nData has been cleaned and filtered for investor use, showing only recent property sales with confirmed prices.",
        #     files=output_files
        # )
        # print(f"[DEBUG] Email sent with {len(output_files)} attachments")
    else:
        print("[DEBUG] No files to send, no email sent.")