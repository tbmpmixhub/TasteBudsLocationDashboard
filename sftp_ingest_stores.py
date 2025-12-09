# This file will run on a schedule to retrieve files from Toast and upload data to Postgres Database on Digital Ocean
from dotenv import load_dotenv
load_dotenv()

import os
import paramiko
import utils
import pandas as pd
from datetime import datetime, timedelta

HOST = os.getenv("SFTP_HOST")
USERNAME = os.getenv("SFTP_USERNAME")
KEY_PATH = os.getenv("SFTP_KEY_PATH")

# Function to calculate the date string for yesterday
def get_yesterday_string():
    yesterday = datetime.now(datetime.timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")
# Change this to the business date you want to ingest: YYYYMMDD
DATE_STR = get_yesterday_string()  # example


def ingest_all_stores_for_date(date_str: str):
    if not HOST or not USERNAME or not KEY_PATH:
        raise RuntimeError("SFTP env vars missing: SFTP_HOST, SFTP_USERNAME, SFTP_KEY_PATH")

    print(f"Connecting to SFTP for date {date_str}...")

    key = paramiko.RSAKey.from_private_key_file(KEY_PATH)
    transport = paramiko.Transport((HOST, 22))
    transport.connect(username=USERNAME, pkey=key)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        store_folders = sftp.listdir(".")
        print("Found store folders:", store_folders)
    # Iterate over the found store folders
        for store in store_folders:
            remote_dir = f"{store}/{date_str}"
            print(f"\n🔍 Checking store {store} at {remote_dir}...")

            # Does this store even have a folder for that date?
            try:
                files = sftp.listdir(remote_dir)
            except FileNotFoundError:
                print(f"  No data folder for {store} on {date_str}, skipping.")
                continue

            # Locate the two CSVs we care about
            item_remote = None
            mod_remote = None
            # Locate the two CSVs we care about
            for fname in files:
                lower = fname.lower()
                if "itemselectiondetails" in lower:
                    item_remote = f"{remote_dir}/{fname}"
                elif "modifiersselectiondetails" in lower:
                    mod_remote = f"{remote_dir}/{fname}"

            if not item_remote or not mod_remote:
                print(f"  Missing one or both CSVs for {store} on {date_str}, skipping.")
                print(f"  Found files: {files}")
                continue

            print(f"  Using item file: {item_remote}")
            print(f"  Using modifier file: {mod_remote}")

            # Open the remote CSVs as file-like objects and let utils.load_data handle them
            with sftp.open(item_remote, "rb") as items_f, sftp.open(mod_remote, "rb") as mods_f:
                items_df, mods_df = utils.load_data(items_f, mods_f)

            if items_df is None or items_df.empty:
                print(f"  No items data for {store} on {date_str}, skipping.")
                continue

            # Generate the interval report (1-hour intervals by default)
            report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")

            if report_df.empty:
                print(f"  Report is empty for {store} on {date_str}, skipping.")
                continue

            # Use the first row to determine date and location (consistent with your existing logic)
            report_date = items_df["Order Date"].dt.date.iloc[0]
            location = items_df["Location"].iloc[0]

            utils.save_report_data(report_date, location, report_df)
            print(f"  ✅ Saved {len(report_df)} rows for store {store} / location '{location}' on {report_date}.")

    finally:
        sftp.close()
        transport.close()
        print("\nSFTP connection closed.")


if __name__ == "__main__":
    ingest_all_stores_for_date(DATE_STR)
