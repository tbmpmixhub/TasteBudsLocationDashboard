# This file uploads ALL local ToastTest CSVs into the database
from dotenv import load_dotenv
load_dotenv()

import utils
import pandas as pd
from pathlib import Path
from datetime import datetime

# Create base directory path string for local CSV files
BASE_DIR = Path(r"C:\Users\EvanLoria\TasteBudsLocationDashboard\csv_files\ToastTest")

# Find all relevant CSVs
item_files = list(BASE_DIR.rglob("ItemSelectionDetails.csv")) # Get all ItemSelectionDetails.csv files in the Base Directory
mod_files  = list(BASE_DIR.rglob("ModifiersSelectionDetails.csv")) # Get all ModifiersSelectionDetails.csv files in the Base Directory

# Turn into lookup dicts keyed by their parent folder
item_map = {folder.parent: folder for folder in item_files}
mod_map  = {folder.parent: folder for folder in mod_files}

pairs = []

# Loop over every folder with ItemSelectionDetails.csv
for folder, item_path in item_map.items():
    # If there's a matching ModifiersSelectionDetails.csv, add the pair
    if folder in mod_map:
        pairs.append((item_path, mod_map[folder]))
    else:
        print(f"⚠️ Missing ModifiersSelectionDetails.csv in {folder}")

print(f"\nFound {len(pairs)} valid file pairs.\n")

# Process each pair
for item_path, mod_path in pairs:
    print(f"Processing:\n  {item_path}\n  {mod_path}")

    # Load the dataframes
    items_df, mods_df = utils.load_data(str(item_path), str(mod_path))

    if items_df is None or items_df.empty:
        print("  ⛔ No item data, skipping.")
        continue

    # Generate the report
    report_df = utils.generate_report_data(items_df, mods_df, interval_type='1 Hour')

    if report_df.empty:
        print("  ⛔ Empty report, skipping.")
        continue

    # Extract date + location
    report_date = items_df["Order Date"].dt.date.iloc[0]
    location = items_df["Location"].iloc[0]

    print(f"  → Date: {report_date}, Location: {location}, Rows: {len(report_df)}")

    # Upload to DB
    utils.save_report_data(report_date, location, report_df)

    print("  ✅ Saved to DB.\n")

print("\n🎉 Finished uploading all matching ToastTest CSVs.\n")
