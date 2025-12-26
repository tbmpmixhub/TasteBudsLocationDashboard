#This file uploads ALL local ToastTest CSVs into the database whose folder date (YYYYMMDD folder name) falls within a fixed range
from dotenv import load_dotenv
load_dotenv()

import utils
import pandas as pd
from pathlib import Path
from datetime import datetime, date

# This is the folder the files must be in to be processed
BASE_DIR = Path(r"C:\Users\EvanLoria\TasteBudsLocationDashboard\csv_files\ToastTest")

# Set your date range here
# Example call => date(2025, 11, 19) to date(2025, 11, 23)
start_date = date(2025, 12, 1)
end_date   = date(2025, 12, 23)

print(f"\n➡ Filtering folders from {start_date} to {end_date}\n")

# Find CSVs
item_files = list(BASE_DIR.rglob("ItemSelectionDetails.csv"))
mod_files  = list(BASE_DIR.rglob("ModifiersSelectionDetails.csv"))

# Map: parent_folder_path → file_path
item_map = {file_path.parent: file_path for file_path in item_files}
mod_map  = {file_path.parent: file_path for file_path in mod_files}

pairs = []

# Match pairs AND filter by date range
for folder_path, item_csv_path in item_map.items():

    # Skip if no matching modifiers file
    if folder_path not in mod_map:
        print(f"⚠️ Missing ModifiersSelectionDetails.csv in {folder_path}")
        continue

    # Parse folder name as YYYYMMDD
    folder_name = folder_path.name
    try:
        folder_date = datetime.strptime(folder_name, "%Y%m%d").date()
    except ValueError:
        print(f"⚠️ Folder '{folder_name}' is not valid YYYYMMDD — skipping")
        continue

    # Check date range
    if not (start_date <= folder_date <= end_date):
       ## print(f"⏭ Skipping {folder_path} (date {folder_date} outside range)")
        continue

    # Valid pair
    pairs.append((item_csv_path, mod_map[folder_path]))

print(f"\nFound {len(pairs)} valid file pairs in date range.\n")

# === 4) Process each pair ===
for item_path, mod_path in pairs:
    print(f"Processing:\n  {item_path}\n  {mod_path}")

    items_df, mods_df = utils.load_data(str(item_path), str(mod_path))

    if items_df is None or items_df.empty:
        print("  ⛔ No item data, skipping.")
        continue

    # Create report
    report_df = utils.generate_report_data(items_df, mods_df, interval_type="1 Hour")

    if report_df.empty:
        print("  ⛔ Empty report, skipping.")
        continue

    # Extract metadata
    report_date = items_df["Order Date"].dt.date.iloc[0]
    location = items_df["Location"].iloc[0]

    print(f"  → Date: {report_date}, Location: {location}, Rows: {len(report_df)}")

    # Save to DB
    utils.save_report_data(report_date, location, report_df)

    print("  ✅ Saved to DB.\n")

print("\n🎉 Finished uploading all matching CSVs in date range.\n")

