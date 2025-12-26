# Date-specific SFTP ingest: retrieves files from Toast and uploads to DO Postgres
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

SLEEP_SECONDS = 300   # 5 minutes between checks
MAX_ATTEMPTS  = 60    # max retry attempts for this date

SCRIPT_DIR = Path(__file__).resolve().parent

# --------- SET THE DATE YOU WANT HERE ----------
# Must be YYYYMMDD (example: "20250816")
TARGET_DATE_STR = os.getenv("TARGET_DATE_STR", "20251224")
# ----------------------------------------------

def clear_processed_stores_file():
    if PROCESSED_STORES_FILE.exists():
        PROCESSED_STORES_FILE.unlink()
        print(f"🧹 Cleared processed stores file: {PROCESSED_STORES_FILE.name}")

# Use a per-date processed file so backfills don't conflict with other runs
PROCESSED_STORES_FILE = SCRIPT_DIR / f"processed_stores_{TARGET_DATE_STR}.json"


def load_processed_stores() -> set[str]:
    if PROCESSED_STORES_FILE.exists():
        with open(PROCESSED_STORES_FILE, "r") as f:
            try:
                return set(json.load(f))
            except Exception:
                return set()
    return set()


def save_processed_stores(stores: set[str]) -> None:
    with open(PROCESSED_STORES_FILE, "w") as f:
        json.dump(sorted(stores), f)


def ingest_all_stores_for_date_once(date_str: str, processed_stores: set[str]) -> set[str]:
    print(f"\n📡 ingest_all_stores_for_date_once() START for {date_str}", flush=True)

    if not HOST or not USERNAME or not KEY_PATH:
        raise RuntimeError("SFTP env vars missing: SFTP_HOST, SFTP_USERNAME, SFTP_KEY_PATH")

    print(f"\nConnecting to SFTP for date {date_str}...")

    key = paramiko.RSAKey.from_private_key_file(KEY_PATH)
    transport = paramiko.Transport((HOST, 22))
    transport.connect(username=USERNAME, pkey=key)
    sftp = paramiko.SFTPClient.from_transport(transport)

    all_store_folders: set[str] = set()

    try:
        store_folders = sftp.listdir(".")
        print("📂 SFTP ROOT CONTENTS:", store_folders, flush=True)

        if "217184" in store_folders:
            store_folders.remove("217184")

        all_store_folders = set(store_folders)
        print("Found store folders:", store_folders)

        for store in store_folders:
            if store in processed_stores:
                print(f"\n✅ Store {store} already processed earlier, skipping.")
                continue

            remote_dir = f"{store}/{date_str}"
            print(f"\n🔍 Checking store {store} at {remote_dir}...")

            try:
                files = sftp.listdir(remote_dir)
            except FileNotFoundError:
                print(f"  No data folder for {store} on {date_str}, skipping.")
                continue

            item_remote = None
            mod_remote = None
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

            try:
                with sftp.open(item_remote, "rb") as items_f, sftp.open(mod_remote, "rb") as mods_f:
                    items_df, mods_df = utils.load_data(items_f, mods_f)

                if items_df is None or items_df.empty:
                    print(f"  No items data for {store} on {date_str}, skipping.")
                    continue

                report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")
                if report_df.empty:
                    print(f"  Report is empty for {store} on {date_str}, skipping.")
                    continue

                report_date = items_df["Order Date"].dt.date.iloc[0]
                location = items_df["Location"].iloc[0]

                utils.save_report_data(report_date, location, report_df)
                print(f"  ✅ Saved {len(report_df)} rows for store {store} / location '{location}' on {report_date}.")

                processed_stores.add(store)
                save_processed_stores(processed_stores)

            except Exception as e:
                print(f"  ⚠️ Error processing store {store}: {e}")

    finally:
        sftp.close()
        transport.close()
        print("SFTP connection closed.")

    return all_store_folders


def main():
    date_str = TARGET_DATE_STR
    print(f"🚀 Running date-specific ingest for {date_str}", flush=True)

    processed_stores = load_processed_stores()
    print(f"Loaded already-processed stores: {sorted(processed_stores)}")

    attempts = 0
    all_stores_seen: set[str] = set()

    while attempts < MAX_ATTEMPTS:
        attempts += 1
        print(f"\n===== Attempt {attempts} for date {date_str} =====")

        all_store_folders = ingest_all_stores_for_date_once(date_str, processed_stores)

        if not all_stores_seen:
            all_stores_seen = set(all_store_folders)
        else:
            all_stores_seen |= set(all_store_folders)

        remaining = all_stores_seen - processed_stores

        print(f"\nProgress: {len(processed_stores)}/{len(all_stores_seen)} stores processed.")
        if remaining:
            print(f"Remaining stores (missing files or not ready yet): {sorted(remaining)}")
        else:
            print("\n✅ All stores processed for this date. Exiting.")
            break

        print(f"\nNot all stores ready. Sleeping {SLEEP_SECONDS} seconds before next attempt...")
        time.sleep(SLEEP_SECONDS)

    remaining = all_stores_seen - processed_stores
    if remaining:
        print(f"\n⚠️ Finished retries with unprocessed stores for {date_str}: {sorted(remaining)}")
        # Always clear processed stores file after run
    clear_processed_stores_file()


if __name__ == "__main__":
    main()
