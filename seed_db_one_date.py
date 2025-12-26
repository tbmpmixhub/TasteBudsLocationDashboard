# This file uploads ToastTest CSVs into the database for ONE specific date
from dotenv import load_dotenv
load_dotenv()

import utils
import pandas as pd
from pathlib import Path
from datetime import date

# This is the folder the files must be in to be processed
BASE_DIR = Path(r"C:\Users\EvanLoria\TasteBudsLocationDashboard\csv_files\ToastTest")


def ingest_one_date(target_date: date):
    """
    Upload Toast CSVs for ONE specific date (YYYYMMDD folder).
    Example: ingest_one_date(date(2025, 12, 15))
    """

    target_folder = target_date.strftime("%Y%m%d")
    print(f"\n➡ Processing single date: {target_date} ({target_folder})\n")

    item_files = list(BASE_DIR.rglob("ItemSelectionDetails.csv"))
    mod_files  = list(BASE_DIR.rglob("ModifiersSelectionDetails.csv"))

    item_map = {fp.parent: fp for fp in item_files}
    mod_map  = {fp.parent: fp for fp in mod_files}

    processed = False

    for folder_path, item_csv_path in item_map.items():

        # Must match target date folder
        if folder_path.name != target_folder:
            continue

        if folder_path not in mod_map:
            print(f"⚠️ Missing ModifiersSelectionDetails.csv in {folder_path}")
            continue

        mod_csv_path = mod_map[folder_path]

        print(f"Processing:\n  {item_csv_path}\n  {mod_csv_path}")

        items_df, mods_df = utils.load_data(str(item_csv_path), str(mod_csv_path))

        if items_df is None or items_df.empty:
            print("  ⛔ No item data, skipping.")
            continue

        report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")

        if report_df.empty:
            print("  ⛔ Empty report, skipping.")
            continue

        report_date = items_df["Order Date"].dt.date.iloc[0]
        location = items_df["Location"].iloc[0]

        print(f"  → Date: {report_date}, Location: {location}, Rows: {len(report_df)}")

        utils.save_report_data(report_date, location, report_df)

        print("  ✅ Saved to DB.\n")
        processed = True

    if not processed:
        print(f"⛔ No valid data found for {target_folder}")

    print(f"\n🎉 Finished single-date ingest for {target_date}\n")


# =========================
# RUN ON EXECUTION
# =========================

if __name__ == "__main__":
    # CHANGE THIS DATE WHEN YOU NEED A BACKFILL
    ingest_one_date(date(2025, 12, 15))
