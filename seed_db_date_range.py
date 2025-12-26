# This file ingests Toast CSVs from SFTP for a DATE RANGE and uploads to Postgres (DO).
# It retries until all stores are processed (or attempts exhausted) and clears processed_stores.json at the end.
from dotenv import load_dotenv
load_dotenv()

import os
import json
import time
from pathlib import Path
import paramiko
import utils
import pandas as pd
from datetime import datetime, date

HOST = os.getenv("SFTP_HOST")
USERNAME = os.getenv("SFTP_USERNAME")
KEY_PATH = os.getenv("SFTP_KEY_PATH")

SLEEP_SECONDS = 300  # 5 minutes
MAX_ATTEMPTS = 60

SCRIPT_DIR = Path(__file__).resolve().parent
PROCESSED_STORES_FILE = SCRIPT_DIR / "processed_stores.json"

# ---- SET YOUR DATE RANGE HERE ----
start_date = date(2025, 12, 1)
end_date   = date(2025, 12, 23)
# ---------------------------------


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


def clear_processed_stores_file() -> None:
    if PROCESSED_STORES_FILE.exists():
        PROCESSED_STORES_FILE.unlink()
        print(f"🧹 Cleared processed stores file: {PROCESSED_STORES_FILE.name}", flush=True)


def ingest_all_stores_for_range_once(
    start_date: date,
    end_date: date,
    processed_stores: set[str],
) -> set[str]:
    """
    One pass:
      - connect to SFTP
      - list all stores
      - for each unprocessed store, find ANY date folder within [start_date, end_date]
      - if BOTH CSVs exist for a given date folder, process it and mark store processed
    Returns set of store folders seen at SFTP root.
    """
    if not HOST or not USERNAME or not KEY_PATH:
        raise RuntimeError("SFTP env vars missing: SFTP_HOST, SFTP_USERNAME, SFTP_KEY_PATH")

    print(f"\n📡 START pass for range {start_date} → {end_date}", flush=True)

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

        for store in store_folders:
            if store in processed_stores:
                print(f"\n✅ Store {store} already processed earlier, skipping.", flush=True)
                continue

            print(f"\n🔍 Checking store {store}...", flush=True)

            # List date folders under store
            try:
                date_folders = sftp.listdir(store)
            except Exception as e:
                print(f"  ⚠️ Could not list store folder {store}: {e}", flush=True)
                continue

            # Filter to valid YYYYMMDD folders within range (sorted for deterministic processing)
            valid_dates: list[tuple[str, date]] = []
            for folder_name in date_folders:
                try:
                    folder_date = datetime.strptime(folder_name, "%Y%m%d").date()
                except ValueError:
                    continue
                if start_date <= folder_date <= end_date:
                    valid_dates.append((folder_name, folder_date))

            valid_dates.sort(key=lambda x: x[1])

            if not valid_dates:
                print(f"  No date folders in range for store {store}.", flush=True)
                continue

            # Process the first date folder in range that has both required CSVs
            processed_this_store = False
            for folder_name, folder_date in valid_dates:
                remote_dir = f"{store}/{folder_name}"
                try:
                    files = sftp.listdir(remote_dir)
                except FileNotFoundError:
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
                    continue

                print(f"  ✅ Found files for {store} on {folder_name}", flush=True)
                print(f"    Item: {item_remote}", flush=True)
                print(f"    Mod : {mod_remote}", flush=True)

                try:
                    with sftp.open(item_remote, "rb") as items_f, sftp.open(mod_remote, "rb") as mods_f:
                        items_df, mods_df = utils.load_data(items_f, mods_f)

                    if items_df is None or items_df.empty:
                        print(f"    ⛔ No items data for {store} on {folder_name}, skipping.", flush=True)
                        continue

                    report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")
                    if report_df.empty:
                        print(f"    ⛔ Empty report for {store} on {folder_name}, skipping.", flush=True)
                        continue

                    report_date = items_df["Order Date"].dt.date.iloc[0]
                    location = items_df["Location"].iloc[0]

                    utils.save_report_data(report_date, location, report_df)
                    print(
                        f"    ✅ Saved {len(report_df)} rows for store {store} / location '{location}' on {report_date}.",
                        flush=True,
                    )

                    processed_stores.add(store)
                    save_processed_stores(processed_stores)
                    processed_this_store = True
                    break  # done with this store

                except Exception as e:
                    print(f"    ⚠️ Error processing store {store} on {folder_name}: {e}", flush=True)
                    # don't mark processed; allow retry

            if not processed_this_store:
                print(f"  Missing required CSV pairs in-range for store {store} (not ready yet).", flush=True)

    finally:
        sftp.close()
        transport.close()
        print("SFTP connection closed.", flush=True)

    return all_store_folders


def main():
    print(f"\n➡ Filtering SFTP folders from {start_date} to {end_date}\n", flush=True)

    processed_stores = load_processed_stores()
    print(f"Loaded already-processed stores: {sorted(processed_stores)}", flush=True)

    attempts = 0
    all_stores_seen: set[str] = set()

    try:
        while attempts < MAX_ATTEMPTS:
            attempts += 1
            print(f"\n===== Attempt {attempts} for range {start_date} → {end_date} =====", flush=True)

            all_store_folders = ingest_all_stores_for_range_once(start_date, end_date, processed_stores)
            if not all_stores_seen:
                all_stores_seen = set(all_store_folders)
            else:
                all_stores_seen |= set(all_store_folders)

            remaining = all_stores_seen - processed_stores
            print(f"\nProgress: {len(processed_stores)}/{len(all_stores_seen)} stores processed.", flush=True)

            if remaining:
                print(f"Remaining stores (missing files or not ready yet): {sorted(remaining)}", flush=True)
                print(f"\nNot all stores ready. Sleeping {SLEEP_SECONDS} seconds before next attempt...", flush=True)
                time.sleep(SLEEP_SECONDS)
            else:
                print("\n✅ All stores processed for this range. Exiting.", flush=True)
                break

        # Final status
        remaining = all_stores_seen - processed_stores
        if remaining:
            print(
                f"\n⚠️ Finished retries with unprocessed stores for range {start_date} → {end_date}: "
                f"{sorted(remaining)}",
                flush=True,
            )
        else:
            print("\n🎉 Finished uploading all matching SFTP CSVs in date range.\n", flush=True)

    finally:
        # Always clear the processed_stores.json file at the end
        clear_processed_stores_file()


if __name__ == "__main__":
    main()
