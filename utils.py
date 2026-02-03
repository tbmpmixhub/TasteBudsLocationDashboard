import pandas as pd
import streamlit as st
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
import time

# Database connection with connection pooling
DATABASE_URL = os.environ.get('DATABASE_URL')
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_pre_ping=True  # Enable connection testing before use
)

def get_db_connection():
    """Get database connection with retry logic"""
    max_retries = 3
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            conn = engine.connect()
            # Test the connection
            conn.execute(text("SELECT 1"))
            return conn
        except SQLAlchemyError as e:
            if attempt == max_retries - 1:
                st.error(f"Failed to connect to database after {max_retries} attempts: {str(e)}")
                raise
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff

def init_db():
    """Initialize database tables with optimized indexes"""
    try:
        # Use a transaction to ensure atomic operation
        with engine.begin() as conn:
            # Create main table with constraints
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS new_sales_data (
                    id SERIAL PRIMARY KEY,
                    location TEXT NOT NULL,
                    order_date DATE NOT NULL,
                    service TEXT NOT NULL,
                    interval_time TEXT NOT NULL,
                    half_chix INTEGER NOT NULL DEFAULT 0,
                    half_ribs INTEGER NOT NULL DEFAULT 0,
                    full_ribs INTEGER NOT NULL DEFAULT 0,
                    six_oz_mod INTEGER NOT NULL DEFAULT 0,
                    eight_oz_mod INTEGER NOT NULL DEFAULT 0,
                    corn INTEGER NOT NULL DEFAULT 0,
                    grits INTEGER NOT NULL DEFAULT 0,
                    pots INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (location, order_date, service, interval_time)
                );

                -- Create indexes if they don't exist
                CREATE INDEX IF NOT EXISTS idx_date_location ON new_sales_data(order_date, location);
                CREATE INDEX IF NOT EXISTS idx_service ON new_sales_data(service);
                CREATE INDEX IF NOT EXISTS idx_interval ON new_sales_data(interval_time);
            """))
    except SQLAlchemyError as e:
        st.error(f"Database initialization error: {str(e)}")
        raise

def save_report_data(date, location, report_df):
    """Save report data to database with improved error handling
    
    This function saves report data for a specific date and location.
    If data for the same date and location already exists, it will be replaced.
    Data for other locations on the same date is preserved.
    
    Args:
        date: The date of the report data
        location: The location name for this report data
        report_df: DataFrame containing the report data
    """
    if report_df.empty:
        return

    # Skip excluded locations (normalize for safety)
    normalized_location = location.strip().lower()
    excluded_locations = {
        "8301 oak st",
        "8301 oak street",  # include this because of past string issues
    }

    if normalized_location in excluded_locations:
        print(f"⚠️ Skipping save for excluded location: {location!r}")
        return
    
    # TEMP: add this line just for testing
    print(">>> REACHED DB BLOCK <<<")
    
    try:
        with engine.begin() as conn:  # Using transaction
            # Only delete existing data for this specific date and location combination
            # This preserves data for other locations on the same date
            conn.execute(text("""
                DELETE FROM new_sales_data 
                WHERE order_date = :date AND location = :location
            """), {
                'date': date,
                'location': location
            })

            # Insert new data
            for _, row in report_df.iterrows():
                conn.execute(text("""
                    INSERT INTO new_sales_data 
                    (location, order_date, service, interval_time, 
                    half_chix, half_ribs, full_ribs, six_oz_mod, eight_oz_mod,
                    corn, grits, pots, total)
                    VALUES 
                    (:location, :order_date, :service, :interval_time,
                    :half_chix, :half_ribs, :full_ribs, :six_oz_mod, :eight_oz_mod,
                    :corn, :grits, :pots, :total)
                    ON CONFLICT (location, order_date, service, interval_time)
                    DO UPDATE SET
                        half_chix = EXCLUDED.half_chix,
                        half_ribs = EXCLUDED.half_ribs,
                        full_ribs = EXCLUDED.full_ribs,
                        six_oz_mod = EXCLUDED.six_oz_mod,
                        eight_oz_mod = EXCLUDED.eight_oz_mod,
                        corn = EXCLUDED.corn,
                        grits = EXCLUDED.grits,
                        pots = EXCLUDED.pots,
                        total = EXCLUDED.total
                """), {
                    'location': location,
                    'order_date': date,
                    'service': row['Service'],
                    'interval_time': row['Interval'],
                    'half_chix': row['1/2 Chix'],
                    'half_ribs': row['1/2 Ribs'],
                    'full_ribs': row['Full Ribs'],
                    'six_oz_mod': row['6oz Mod'],
                    'eight_oz_mod': row['8oz Mod'],
                    'corn': row['Corn'],
                    'grits': row['Grits'],
                    'pots': row['Pots'],
                    'total': row['Total']
                })
    except SQLAlchemyError as e:
        st.error(f"Error saving data: {str(e)}")
        raise

def get_report_data(date, location, interval_type='1 Hour'):
    """Retrieve report data from database with optional interval type conversion"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT service as "Service",
                       interval_time as "Interval",
                       half_chix as "1/2 Chix",
                       half_ribs as "1/2 Ribs",
                       full_ribs as "Full Ribs",
                       six_oz_mod as "6oz Mod",
                       eight_oz_mod as "8oz Mod",
                       corn as "Corn",
                       grits as "Grits",
                       pots as "Pots",
                       total as "Total"
                FROM new_sales_data
                WHERE order_date = :date
                AND location = :location
                ORDER BY 
                    CASE service 
                        WHEN 'Lunch' THEN 1 
                        WHEN 'Dinner' THEN 2 
                    END,
                    interval_time
            """), {'date': date, 'location': location})

            df = pd.DataFrame(result.fetchall())
            if not df.empty:
                df = df.sort_values(['Service', 'Interval'])
                
                # If 30-minute intervals are requested, convert the 1-hour data
                if interval_type == '30 Minutes' and not df.empty:
                    df = convert_to_30min_intervals(df)
            
            return df
    except SQLAlchemyError as e:
        st.error(f"Error retrieving data: {str(e)}")
        return pd.DataFrame()

def convert_to_30min_intervals(hourly_df):
    """Convert 1-hour interval data to 30-minute intervals by splitting each hour's data"""
    if hourly_df.empty:
        return hourly_df
    
    result_rows = []
    
    # Process each row in the hourly data
    for _, row in hourly_df.iterrows():
        service = row['Service']
        hour_str = row['Interval']
        
        # Skip totals or any non-hour format rows
        if not ':' in str(hour_str) or 'Total' in str(service):
            result_rows.append(row.to_dict())
            continue
            
        # Parse the hour
        try:
            hour = int(hour_str.split(':')[0])
            
            # Create two 30-minute intervals for this hour
            # First half of the hour (XX:00)
            first_half = row.copy()
            first_half['Interval'] = f"{hour:02d}:00"
            
            # Distribute values (approximately half to each interval)
            numeric_cols = ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 
                           'Corn', 'Grits', 'Pots', 'Total']
            
            for col in numeric_cols:
                # Split the count evenly between the two 30-minute intervals
                # (slightly favoring the first half for odd numbers)
                first_half[col] = int(row[col] / 2 + 0.5)
                
            # Second half of the hour (XX:30)
            second_half = row.copy()
            second_half['Interval'] = f"{hour:02d}:30"
            
            for col in numeric_cols:
                # The second half gets the remainder
                second_half[col] = row[col] - first_half[col]
                
            # Add both intervals to the results
            result_rows.append(first_half.to_dict())
            result_rows.append(second_half.to_dict())
            
        except (ValueError, IndexError):
            # If there's any error parsing the interval, just keep the original row
            result_rows.append(row.to_dict())
    
    # Convert back to DataFrame and sort
    result_df = pd.DataFrame(result_rows)
    
    if not result_df.empty:
        result_df = result_df.sort_values(['Service', 'Interval'])
    
    return result_df

def get_available_locations_and_dates():
    """Retrieve available locations and dates from database
    
    Returns:
        tuple: (locations, dates)
            - locations: Sorted list of unique location names
            - dates: Sorted list of unique dates
    
    This function retrieves all distinct location names and dates from the database.
    It's used to populate filter dropdowns and ensure all historical data is accessible.
    """
    try:
        with engine.connect() as conn:
            # Get all distinct locations and dates as separate queries
            # This ensures we get all possible combinations even if some dates don't have all locations
            locations_result = conn.execute(text("""
                SELECT DISTINCT location
                FROM new_sales_data
                ORDER BY location
            """))
            
            dates_result = conn.execute(text("""
                SELECT DISTINCT order_date
                FROM new_sales_data
                ORDER BY order_date DESC
            """))

            # Extract and sort the results
            locations = sorted([row[0] for row in locations_result.fetchall()])
            dates = sorted([row[0] for row in dates_result.fetchall()])

            return locations, dates
    except SQLAlchemyError as e:
        st.error(f"Error retrieving locations and dates: {str(e)}")
        return [], []

def load_data(items_file, modifiers_file):
    """Load and preprocess sales data from CSV files"""
    try:
        # Read CSV files
        items_df = pd.read_csv(items_file)
        modifiers_df = pd.read_csv(modifiers_file)

        # Ensure string columns are properly handled
        string_columns = ['Menu Item', 'Modifier', 'Parent Menu Selection', 'Location', 'Void?']
        for df in [items_df, modifiers_df]:
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)

        # Convert date columns to datetime
        items_df['Order Date'] = pd.to_datetime(items_df['Order Date'])
        modifiers_df['Order Date'] = pd.to_datetime(modifiers_df['Order Date'])

        # Convert Qty to numeric, handling any non-numeric values
        items_df['Qty'] = pd.to_numeric(items_df['Qty'].replace({'false': '0', 'true': '0'}), errors='coerce').fillna(0)
        modifiers_df['Qty'] = pd.to_numeric(modifiers_df['Qty'].replace({'false': '0', 'true': '0'}), errors='coerce').fillna(0)

        # Filter out void items (convert to lowercase for comparison)
        items_df = items_df[items_df['Void?'].str.lower().replace({'nan': 'false'}) != 'true']
        modifiers_df = modifiers_df[modifiers_df['Void?'].str.lower().replace({'nan': 'false'}) != 'true']
        
        # Handle PLU column for items (Different CSVs might have different column names)
        if 'PLU' in items_df.columns:
            # PLU column exists, convert to numeric for comparison
            items_df['PLU'] = pd.to_numeric(items_df['PLU'], errors='coerce')
        else:
            # Try to find an alternative column based on spreadsheet mappings
            # Checking both 'Column P in Items CSV' and 'Master Id' as possible sources
            if 'Master Id' in items_df.columns:
                items_df['PLU'] = pd.to_numeric(items_df['Master Id'], errors='coerce')
            
        # Handle PLU column for modifiers
        # We want to check both PLU and Modifier PLU columns
        if 'Modifier PLU' in modifiers_df.columns:
            # Modifier PLU column exists, convert to numeric for comparison
            modifiers_df['PLU'] = pd.to_numeric(modifiers_df['Modifier PLU'], errors='coerce')
        elif 'PLU' in modifiers_df.columns:
            # PLU column exists, convert to numeric for comparison
            modifiers_df['PLU'] = pd.to_numeric(modifiers_df['PLU'], errors='coerce')
        else:
            # Try to find an alternative column based on spreadsheet mappings
            if 'Master Id' in modifiers_df.columns:
                modifiers_df['PLU'] = pd.to_numeric(modifiers_df['Master Id'], errors='coerce')

        return items_df, modifiers_df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None

def calculate_interval_counts(interval_items, interval_mods):
    """Calculate counts for a specific interval based on PLU mappings"""
    # Initialize counts
    counts = {
        '1/2 Chix': 0, '1/2 Ribs': 0, 'Full Ribs': 0,
        '6oz Mod': 0, '8oz Mod': 0,
        'Corn': 0, 'Grits': 0, 'Pots': 0
    }

    # Define PLU mappings for each category based on the updated spreadsheets
    # IMPORTANT: When updating PLUs, make sure to include existing PLUs and add new ones at the end
    # PLUs can be sourced from either 'PLU' column in Items CSV or 'Modifier PLU' in Modifiers CSV
    plu_mapping = {
        # 1/2 Chicken category PLUs
        '1/2 Chix': [81831, 81990, 81991, 3074, 3001, 3009, 81828, 82316, 81783, 82445],
        
        # 1/2 Ribs category PLUs (includes PLU 2007 as requested)
        '1/2 Ribs': [82151, 82149, 82147, 3033, 3034, 3032, 81912, 3009, 2007, 82152, 82150, 82148],
        
        # Full Ribs category PLUs
        'Full Ribs': [2273, 2276, 2280, 81831, 81830],
        
        # 6oz Mod category PLUs
        '6oz Mod': [3316, 3418, 81785],
        
        # 8oz Mod category PLUs
        '8oz Mod': [81829, 2114],
        
        # Corn category PLUs
        'Corn': [2307, 3082, 3648, 2303],
        
        # Grits category PLUs
        'Grits': [2308, 3086, 3618, 2306],
        
        # Pots category PLUs
        'Pots': [2310, 3081, 3622, 2309],
        
      
    }

    # Process items data
    if not interval_items.empty:
        # Ensure 'PLU' column is numeric or use string comparison if needed
        if 'PLU' in interval_items.columns:
            # Make a copy to avoid SettingWithCopyWarning
            items_df = interval_items.copy()
            
            # Try to convert to numeric for safer comparison
            try:
                items_df['PLU'] = pd.to_numeric(items_df['PLU'], errors='coerce')
                
                # TEMP debug for new PLU
                print("82445 item matches:", items_df[items_df["PLU"] == 82445]["Qty"].sum())

                # Count each category based on PLU, using Qty values
                for category, plus in plu_mapping.items():
                    category_counts = items_df[
                        items_df['PLU'].isin(plus)
                    ]['Qty'].sum()
                    counts[category] += category_counts
            except:
                # If numeric conversion fails, use string comparison
                for category, plus in plu_mapping.items():
                    category_counts = items_df[
                        items_df['PLU'].astype(str).isin([str(plu) for plu in plus])
                    ]['Qty'].sum()
                    counts[category] += category_counts

    # Process modifiers data
    if not interval_mods.empty:
        # Make a copy to avoid SettingWithCopyWarning
        mods_df = interval_mods.copy()
        
        # For newer CSV format, check if Modifier PLU exists first
        if 'Modifier PLU' in mods_df.columns:
            try:
                # Create a new merged PLU column that combines both sources
                mods_df['PLU_Combined'] = pd.to_numeric(mods_df['Modifier PLU'], errors='coerce')
                
                # Count each category based on the combined PLU field
                for category, plus in plu_mapping.items():
                    category_counts = mods_df[
                        mods_df['PLU_Combined'].isin(plus)
                    ]['Qty'].sum()
                    counts[category] += category_counts
            except:
                # If numeric conversion fails, use string comparison
                for category, plus in plu_mapping.items():
                    category_counts = mods_df[
                        mods_df['Modifier PLU'].astype(str).isin([str(plu) for plu in plus])
                    ]['Qty'].sum()
                    counts[category] += category_counts
        # For older format or if no Modifier PLU present, use PLU column
        elif 'PLU' in mods_df.columns:
            # Try to convert to numeric for safer comparison
            try:
                mods_df['PLU'] = pd.to_numeric(mods_df['PLU'], errors='coerce')
                
                # Count each category based on PLU
                for category, plus in plu_mapping.items():
                    category_counts = mods_df[
                        mods_df['PLU'].isin(plus)
                    ]['Qty'].sum()
                    counts[category] += category_counts
            except:
                # If numeric conversion fails, use string comparison
                for category, plus in plu_mapping.items():
                    category_counts = mods_df[
                        mods_df['PLU'].astype(str).isin([str(plu) for plu in plus])
                    ]['Qty'].sum()
                    counts[category] += category_counts

    # Calculate total
    counts['Total'] = sum(counts.values())

    return {k: int(v) for k, v in counts.items()}

def generate_report_data(items_df, modifiers_df=None, interval_type='1 Hour'):
    """Generate report data with quantity-based counting and flexible interval options"""
    if items_df is None or items_df.empty:
        return pd.DataFrame()

    report_data = []
    dates = sorted(items_df['Order Date'].dt.date.unique())
    
    # Set interval details based on interval type
    minute_step = 30 if interval_type == '30 Minutes' else 60
    
    for date in dates:
        # Filter data for current date
        date_items = items_df[items_df['Order Date'].dt.date == date]

        if modifiers_df is not None:
            date_mods = modifiers_df[modifiers_df['Order Date'].dt.date == date]
        else:
            date_mods = pd.DataFrame()

        # Process each service period
        for service in ['Lunch', 'Dinner']:
            # Define service hours
            start_hour = 6 if service == 'Lunch' else 16
            end_hour = 16 if service == 'Lunch' else 24

            # Filter by service period
            service_items = date_items[
                (date_items['Order Date'].dt.hour >= start_hour) &
                (date_items['Order Date'].dt.hour < end_hour)
            ]

            if not date_mods.empty:
                service_mods = date_mods[
                    (date_mods['Order Date'].dt.hour >= start_hour) &
                    (date_mods['Order Date'].dt.hour < end_hour)
                ]
            else:
                service_mods = pd.DataFrame()
            
            # Process intervals based on selected interval type
            if interval_type == '1 Hour':
                # Process each hour
                for hour in range(start_hour, end_hour):
                    # Filter data for current hour
                    interval_items = service_items[service_items['Order Date'].dt.hour == hour]
                    interval_mods = service_mods[service_mods['Order Date'].dt.hour == hour] if not service_mods.empty else pd.DataFrame()

                    # Calculate counts
                    counts = calculate_interval_counts(interval_items, interval_mods)

                    if sum(counts.values()) > 0:
                        report_data.append({
                            'Service': service,
                            'Interval': f"{hour:02d}:00",
                            **counts
                        })
            else:  # 30 Minutes
                # Process in 30-minute intervals
                for hour in range(start_hour, end_hour):
                    for minute in [0, 30]:
                        # Calculate start and end times for the 30-minute interval
                        start_time = pd.Timestamp(date).replace(hour=hour, minute=minute)
                        end_time = start_time + pd.Timedelta(minutes=30)
                        
                        # Filter data for current 30-minute interval
                        interval_items = service_items[
                            (service_items['Order Date'] >= start_time) &
                            (service_items['Order Date'] < end_time)
                        ]
                        
                        interval_mods = pd.DataFrame()
                        if not service_mods.empty:
                            interval_mods = service_mods[
                                (service_mods['Order Date'] >= start_time) &
                                (service_mods['Order Date'] < end_time)
                            ]

                        # Calculate counts
                        counts = calculate_interval_counts(interval_items, interval_mods)

                        if sum(counts.values()) > 0:
                            report_data.append({
                                'Service': service,
                                'Interval': f"{hour:02d}:{minute:02d}",
                                **counts
                            })

    # Create DataFrame and format
    if not report_data:
        return pd.DataFrame()

    report_df = pd.DataFrame(report_data)
    report_df = report_df.sort_values(['Service', 'Interval'])

    # Ensure all numeric columns are integers
    numeric_cols = ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 'Corn', 'Grits', 'Pots', 'Total']
    report_df[numeric_cols] = report_df[numeric_cols].fillna(0).astype(int)

    return report_df