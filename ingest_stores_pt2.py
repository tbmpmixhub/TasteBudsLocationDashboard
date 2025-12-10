# This file will run on a schedule to retrieve files from Toast and upload data to Postgres Database on Digital Ocean
from dotenv import load_dotenv
load_dotenv()

import os
import json
import time
from pathlib import Path
import paramiko
import utils
import pandas as pd
from datetime import datetime, timedelta, timezone

HOST = os.getenv("SFTP_HOST")
USERNAME = os.getenv("SFTP_USERNAME")
KEY_PATH = os.getenv("SFTP_KEY_PATH")

# Store ingest retry settings
SLEEP_SECONDS = 300   # 5 minutes between checks
MAX_ATTEMPTS  = 60    # max retry attempts for this date

# processed_stores.json in same folder as this script
SCRIPT_DIR = Path(__file__).resolve().parent
PROCESSED_STORES_FILE = SCRIPT_DIR / "processed_stores.json"

# Function to calculate the date string for yesterday
def get_yesterday_string() -> str:
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")

# This represents the date of the data to ingest
DATE_STR = get_yesterday_string()


# ---- PROCESSED STORES HELPERS ----

# Load previpously processed stores from JSON file
def load_processed_stores() -> set[str]:
    # Check whether the JSON file exists
    if PROCESSED_STORES_FILE.exists():
        # open and read the JSON file
        with open(PROCESSED_STORES_FILE, "r") as f:
            try:
                # convert list of stores to a set for faster lookup
                return set(json.load(f))
            # return empty set on error
            except Exception:
                return set()
    # return an empty set if file does not exist
    return set()

# Save the updated list of processed stores to JSON file
def save_processed_stores(stores: set[str]) -> None:
    # open the JSON file in write mode, overwriting existing content
    with open(PROCESSED_STORES_FILE, "w") as f:
        # write the sorted list of stores to the file
        json.dump(sorted(stores), f)


# ---- SINGLE-ATTEMPT INGEST ----

def ingest_all_stores_for_date_once(date_str: str, processed_stores: set[str]) -> set[str]:
    """
    Run one pass:
      - connect to SFTP
      - list all store folders
      - process any store that:
          * isn't in processed_stores
          * has both CSVs for the date
    Returns the set of all store folders seen on SFTP (for this date run).
    """
    # Make sure all env vars are set
    if not HOST or not USERNAME or not KEY_PATH:
        raise RuntimeError("SFTP env vars missing: SFTP_HOST, SFTP_USERNAME, SFTP_KEY_PATH")

    print(f"\nConnecting to SFTP for date {date_str}...")

    # --- Setup SFTP connection setup ---
    # Load the private key from KEY_PATH
    key = paramiko.RSAKey.from_private_key_file(KEY_PATH)
    transport = paramiko.Transport((HOST, 22))
    transport.connect(username=USERNAME, pkey=key)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Initialize all_store_folders as an empty set
    all_store_folders: set[str] = set()

    try:
        # List all store folders on SFTP
        store_folders = sftp.listdir(".")
        # Set all_store_folders equals to the set of store_folders found
        all_store_folders = set(store_folders)
        print("Found store folders:", store_folders)

        # Iterate over the found store folders
        for store in store_folders:
            # Skip if this store already processed in a previous attempt
            if store in processed_stores:
                print(f"\n✅ Store {store} already processed earlier, skipping.")
                continue
            # Build the remote directory path for this store and date
            remote_dir = f"{store}/{date_str}"
            print(f"\n🔍 Checking store {store} at {remote_dir}...")

            # Check if the store has a folder for that date
            try:
                files = sftp.listdir(remote_dir)
            # Skip if the folder for that date does not exist
            except FileNotFoundError:
                print(f"  No data folder for {store} on {date_str}, skipping.")
                continue

            # Locate the two CSVs we care about
            item_remote = None
            mod_remote = None
            for fname in files:
                lower = fname.lower()
                # If filename contains the relevant keywords, set the remote paths
                if "itemselectiondetails" in lower:
                    item_remote = f"{remote_dir}/{fname}"
                elif "modifiersselectiondetails" in lower:
                    mod_remote = f"{remote_dir}/{fname}"
            # Skip if one or both CSVs are missing
            if not item_remote or not mod_remote:
                print(f"  Missing one or both CSVs for {store} on {date_str}, skipping.")
                print(f"  Found files: {files}")
                continue

            print(f"  Using item file: {item_remote}")
            print(f"  Using modifier file: {mod_remote}")

            # Open the remote CSVs as file-like objects and let utils.load_data handle them
            try:
                # Use sftp.open() to open CSV files in binary read mode
                with sftp.open(item_remote, "rb") as items_f, sftp.open(mod_remote, "rb") as mods_f:
                    items_df, mods_df = utils.load_data(items_f, mods_f)
                # Skip if items_df is None or empty
                if items_df is None or items_df.empty:
                    print(f"  No items data for {store} on {date_str}, skipping.")
                    continue

                # Generate the interval report (1-hour intervals by default)
                report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")
                # Skip if the generated report is empty
                if report_df.empty:
                    print(f"  Report is empty for {store} on {date_str}, skipping.")
                    continue

                # Use the first row to determine date and location
                report_date = items_df["Order Date"].dt.date.iloc[0]
                location = items_df["Location"].iloc[0]

                utils.save_report_data(report_date, location, report_df)
                print(f"  ✅ Saved {len(report_df)} rows for store {store} / location '{location}' on {report_date}.")

                # Mark this store as processed and persist to JSON
                processed_stores.add(store)
                save_processed_stores(processed_stores)

            except Exception as e:
                print(f"  ⚠️ Error processing store {store}: {e}")
                # don't mark as processed on error, so it can retry later

    finally:
        sftp.close()
        transport.close()
        print("SFTP connection closed.")
    # Returns the set of store folder names that existed at the SFTP root when this pass ran
    return all_store_folders


# ---- MAIN RETRY LOOP ----

def main():
    date_str = DATE_STR
    # Set processed_stores by loading from JSON file
    processed_stores = load_processed_stores()
    print(f"Loaded already-processed stores: {sorted(processed_stores)}")

    # Initialize attempt counter and all_stores_seen set
    attempts = 0
    all_stores_seen: set[str] = set()
    # While attempts is less than MAX_ATTEMPTS
    while attempts < MAX_ATTEMPTS:
        # Increment attempt counter
        attempts += 1
        print(f"\n===== Attempt {attempts} for date {date_str} =====")

        # Run one ingest pass
        all_store_folders = ingest_all_stores_for_date_once(date_str, processed_stores)
        # If all_stores_seen is empty, set it to all_store_folders returned from pass
        if not all_stores_seen:
            all_stores_seen = set(all_store_folders)
        else:
            # Update all_stores_seen with any new stores found this pass
            all_stores_seen |= set(all_store_folders)
        # Determine remaining unprocessed stores
        remaining = all_stores_seen - processed_stores

        print(
            f"\nProgress: {len(processed_stores)}/{len(all_stores_seen)} stores processed."
        )
        # If there are remaining stores, print them; otherwise, exit loop
        if remaining:
            print(f"Remaining stores (missing files or not ready yet): {sorted(remaining)}")
        else:
            print("\n✅ All stores processed for this date. Exiting.")
            break

        print(f"\nNot all stores ready. Sleeping {SLEEP_SECONDS} seconds before next attempt...")
        time.sleep(SLEEP_SECONDS)

    # Final check after loop
    remaining = all_stores_seen - processed_stores
    if remaining:
        print(
            f"\n⚠️ Finished retries with unprocessed stores for {date_str}: "
            f"{sorted(remaining)}"
        )


if __name__ == "__main__":
    main()
