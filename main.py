import streamlit as st
import pandas as pd
from PIL import Image
import sys
import traceback
from dotenv import load_dotenv
load_dotenv()
import utils


# Configure Streamlit page
st.set_page_config(
    page_title="Sales Count Report",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'initialization_completed' not in st.session_state:
    st.session_state.initialization_completed = False

# Initialize database connection on startup
if not st.session_state.initialization_completed:
    try:
        utils.init_db()
        st.session_state.initialization_completed = True
    except Exception as e:
        st.error("Failed to initialize database. Please try again later.")
        st.error(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

# Initialize session state for data
if 'items_df' not in st.session_state:
    st.session_state.items_df = None
if 'modifiers_df' not in st.session_state:
    st.session_state.modifiers_df = None

# Load available locations and dates from database
try:
    db_locations, db_dates = utils.get_available_locations_and_dates()
except Exception as e:
    st.error(f"Error loading locations and dates: {str(e)}")
    db_locations, db_dates = [], []

# Initialize locations and selected_location if not in session state
if 'locations' not in st.session_state:
    st.session_state.locations = db_locations
if 'selected_location' not in st.session_state:
    st.session_state.selected_location = db_locations[0] if db_locations else None

# File Upload section
st.sidebar.title('File Upload')

# Action buttons at the top of sidebar
col1, col2 = st.sidebar.columns(2)
if col1.button('Clear All Data', type='primary', use_container_width=True):
    st.session_state.items_df = None
    st.session_state.modifiers_df = None
    st.rerun()

# Initialize clear_upload_fields session state if not exists 
if 'clear_upload_fields' not in st.session_state:
    st.session_state.clear_upload_fields = False

# Button to reset form for uploading another location's data
upload_another_btn = st.sidebar.button('Upload Another Location', 
                                      type='secondary', 
                                      help="Clear the upload form to add data for another location",
                                      use_container_width=True)

if upload_another_btn or st.session_state.clear_upload_fields:
    # Reset the form variables but keep existing data
    st.session_state.clear_upload_fields = False
    # This forces the file uploader widgets to reset without clearing the data
    if 'widget_key' not in st.session_state:
        st.session_state.widget_key = 0
    st.session_state.widget_key += 1

if col2.button('Recalculate Data', type='secondary', use_container_width=True):
    if st.session_state.items_df is not None and st.session_state.modifiers_df is not None:
        # Show recalculation status
        recalc_status = st.sidebar.status("Recalculating historical data...")
        
        # Add a debug expander to show PLU info
        debug_info = st.sidebar.expander("🔄 Recalculation Debug Info", expanded=False)
        with debug_info:
            st.write("**PLU Data Sources:**")
            # Check for Modifier PLU column in modifiers dataframe
            if 'Modifier PLU' in st.session_state.modifiers_df.columns:
                st.success("Using 'Modifier PLU' column from Modifiers CSV")
                sample_plus = st.session_state.modifiers_df['Modifier PLU'].dropna().head(5).tolist()
                st.write(f"Sample PLUs: {sample_plus}")
            elif 'PLU' in st.session_state.modifiers_df.columns:
                st.warning("No 'Modifier PLU' column found. Using 'PLU' instead.")
                sample_plus = st.session_state.modifiers_df['PLU'].dropna().head(5).tolist()
                st.write(f"Sample PLUs: {sample_plus}")
            else:
                st.error("No PLU columns found in Modifiers CSV. Check data format.")
                
            # Show locations in the data
            locations = sorted(st.session_state.items_df['Location'].unique())
            st.write(f"**Locations in data:** {', '.join(locations)}")
        
        # Get all available dates for recalculation
        dates = sorted(set(st.session_state.items_df['Order Date'].dt.date))
        locations = sorted(st.session_state.items_df['Location'].unique())
        
        # If a specific location is selected, only recalculate for that location
        if st.session_state.selected_location and st.session_state.selected_location in locations:
            locations_to_process = [st.session_state.selected_location]
            recalc_status.update(label=f"Recalculating data for {st.session_state.selected_location} only")
            with debug_info:
                st.info(f"Filtering recalculation to only process data for: {st.session_state.selected_location}")
        else:
            locations_to_process = locations
            with debug_info:
                st.info(f"Processing all locations: {', '.join(locations)}")
        
        # Recalculate for each date and location
        for date in dates:
            recalc_status.update(label=f"Recalculating data for {date}")
            for location in locations_to_process:
                # Filter data for date and location
                date_items = st.session_state.items_df[
                    (st.session_state.items_df['Order Date'].dt.date == date) &
                    (st.session_state.items_df['Location'] == location)
                ]
                date_mods = st.session_state.modifiers_df[
                    (st.session_state.modifiers_df['Order Date'].dt.date == date) &
                    (st.session_state.modifiers_df['Location'] == location)
                ]
                
                # Generate and save report data with updated PLU calculations
                report_df = utils.generate_report_data(date_items, date_mods, interval_type='1 Hour')
                if not report_df.empty:
                    utils.save_report_data(date, location, report_df)
                    with debug_info:
                        st.write(f"✅ Successfully calculated for {date} at {location}")
                else:
                    with debug_info:
                        st.write(f"⚠️ No data generated for {date} at {location}")
        
        # Complete recalculation
        recalc_status.update(label="Recalculation complete!", state="complete")
    else:
        st.sidebar.warning("No data available to recalculate. Please upload data files first.")

# File upload section
st.sidebar.subheader("Upload Location Data")
# Get a unique key for the upload form
widget_key = st.session_state.get('widget_key', 0)

location_label = st.sidebar.text_input("Location Name for Upload (optional)", 
                                     key=f"location_name_{widget_key}",
                                     help="Enter the location name for the files you're uploading. Leave blank to use the location in the CSV.")

items_file = st.sidebar.file_uploader("Upload Items CSV for Location", 
                                     type=['csv'],
                                     key=f"items_csv_{widget_key}")
                                     
modifiers_file = st.sidebar.file_uploader("Upload Modifiers CSV for Location", 
                                        type=['csv'],
                                        key=f"modifiers_csv_{widget_key}")

# Sidebar filters section
st.sidebar.title('Filters')

# Refresh locations from database to include newly pulled data
current_db_locations, current_db_dates = utils.get_available_locations_and_dates()
if current_db_locations:
    st.session_state.locations = sorted(set(current_db_locations))

# Location filter
if st.session_state.locations:
    selected_location = st.sidebar.selectbox(
        'Location',
        options=st.session_state.locations,
        index=st.session_state.locations.index(st.session_state.selected_location) if st.session_state.selected_location and st.session_state.selected_location in st.session_state.locations else 0
    )
    st.session_state.selected_location = selected_location
else:
    st.sidebar.error("No locations available")
    selected_location = None

# Date filter - Use current dates from database including newly pulled data
dates = sorted(set(current_db_dates))
if st.session_state.items_df is not None:
    dates = sorted(set(dates + list(st.session_state.items_df['Order Date'].dt.date.unique())))

if dates:
    selected_date = st.sidebar.date_input(
        'Date',
        value=dates[-1],  # Default to most recent date
        min_value=dates[0],
        max_value=dates[-1]
    )
else:
    st.sidebar.error("No dates available")
    selected_date = None

# Time interval filter
interval_options = ['1 Hour', '30 Minutes']
selected_interval = st.sidebar.radio('Time Interval', interval_options, index=0)

# Load data when files are uploaded
if items_file and modifiers_file:
    try:
        # Load new data
        new_items_df, new_modifiers_df = utils.load_data(items_file, modifiers_file)

        if new_items_df is not None and new_modifiers_df is not None:
            # If user specified a location name, override the location in the data
            if location_label and location_label.strip():
                # Make a copy to ensure we don't get warnings
                new_items_df = new_items_df.copy()
                new_modifiers_df = new_modifiers_df.copy()
                
                # Override location with user-provided location name
                original_locations = sorted(new_items_df['Location'].unique())
                
                new_items_df['Location'] = location_label.strip()
                new_modifiers_df['Location'] = location_label.strip()
                
                st.sidebar.success(f"Changed location from {', '.join(original_locations)} to '{location_label.strip()}'")
            
            # Display data info
            st.sidebar.write(f"Items rows: {len(new_items_df)}")
            st.sidebar.write(f"Modifiers rows: {len(new_modifiers_df)}")
            
            # Get locations from data
            file_locations = sorted(new_items_df['Location'].unique())
            st.sidebar.write(f"Location(s) in files: {', '.join(file_locations)}")
            
            # Display debug info about columns for PLU tracking
            debug_info = st.sidebar.expander("📊 Data Column Info")
            with debug_info:
                st.write("**Items CSV Columns:**")
                if 'PLU' in new_items_df.columns:
                    st.success("Using 'PLU' column from Items CSV")
                    st.write(f"Sample PLUs: {new_items_df['PLU'].dropna().head(5).tolist()}")
                elif 'Master Id' in new_items_df.columns:
                    st.warning("No PLU column found. Using 'Master Id' instead.")
                    st.write(f"Sample Master Ids: {new_items_df['Master Id'].dropna().head(5).tolist()}")
                else:
                    st.error("No PLU or Master Id column found in Items CSV.")
                
                st.write("**Modifiers CSV Columns:**")
                if 'Modifier PLU' in new_modifiers_df.columns:
                    st.success("Using 'Modifier PLU' column from Modifiers CSV")
                    st.write(f"Sample Modifier PLUs: {new_modifiers_df['Modifier PLU'].dropna().head(5).tolist()}")
                elif 'PLU' in new_modifiers_df.columns:
                    st.warning("No Modifier PLU column. Using 'PLU' instead.")
                    st.write(f"Sample PLUs: {new_modifiers_df['PLU'].dropna().head(5).tolist()}")
                elif 'Master Id' in new_modifiers_df.columns:
                    st.warning("No PLU columns found. Using 'Master Id' instead.")
                    st.write(f"Sample Master Ids: {new_modifiers_df['Master Id'].dropna().head(5).tolist()}")
                else:
                    st.error("No PLU or Master Id column found in Modifiers CSV.")

            # Store uploaded data - append to existing data if present
            if st.session_state.items_df is not None and st.session_state.modifiers_df is not None:
                # We already have some data, so append the new data
                st.session_state.items_df = pd.concat([st.session_state.items_df, new_items_df])
                st.session_state.modifiers_df = pd.concat([st.session_state.modifiers_df, new_modifiers_df])
                st.sidebar.success("Added new data to existing data")
            else:
                # First upload, just store the data
                st.session_state.items_df = new_items_df
                st.session_state.modifiers_df = new_modifiers_df

            # Get new locations from uploaded data
            new_locations = sorted(new_items_df['Location'].unique())

            # Update locations list while preserving historical locations
            st.session_state.locations = sorted(set(db_locations + new_locations))

            if st.session_state.selected_location not in st.session_state.locations:
                st.session_state.selected_location = st.session_state.locations[0] if st.session_state.locations else None

            # Generate and save report data for new dates
            new_dates = set(new_items_df['Order Date'].dt.date)
            
            # Check if we should filter processing to a specific location
            locations_to_process = new_locations
            
            # If a location is selected and present in the new data, only process that location
            if st.session_state.selected_location and st.session_state.selected_location in new_locations:
                locations_to_process = [st.session_state.selected_location]
                st.sidebar.info(f"Processing data for selected location: {st.session_state.selected_location}")
            else:
                st.sidebar.info(f"Processing data for all locations: {', '.join(new_locations)}")
            
            # Create an upload status indicator
            upload_status = st.sidebar.status("Processing uploaded data...")
            
            # Process each date and location
            for date in new_dates:
                upload_status.update(label=f"Processing data for {date}")
                for location in locations_to_process:
                    # Filter data for date and location
                    date_items = new_items_df[
                        (new_items_df['Order Date'].dt.date == date) &
                        (new_items_df['Location'] == location)
                    ]
                    date_mods = new_modifiers_df[
                        (new_modifiers_df['Order Date'].dt.date == date) &
                        (new_modifiers_df['Location'] == location)
                    ]

                    # Generate and save report data - default to 1 Hour intervals for storage
                    report_df = utils.generate_report_data(date_items, date_mods, interval_type='1 Hour')
                    if not report_df.empty:
                        utils.save_report_data(date, location, report_df)
                        upload_status.update(label=f"Processed data for {date} at {location}")
            
            # Complete status
            upload_status.update(label="Upload processing complete!", state="complete")
            st.sidebar.success('Files uploaded and processed successfully!')
    except Exception as e:
        st.sidebar.error(f'Error processing files: {str(e)}')

# Display logo
try:
    logo = Image.open('attached_assets/image_1740704103897.png')
    st.image(logo, width=150)
except Exception as e:
    st.error(f"Error loading logo: {str(e)}")

# Display report header
if selected_location:
    st.markdown(f'<h1 class="report-title">{selected_location}</h1>', unsafe_allow_html=True)
else:
    st.markdown(f'<h1 class="report-title">No Location Selected</h1>', unsafe_allow_html=True)

if selected_date:
    st.markdown(f'<h2 class="report-title">{selected_date.strftime("%m/%d/%Y")}</h2>', unsafe_allow_html=True)
else:
    st.markdown(f'<h2 class="report-title">No Date Selected</h2>', unsafe_allow_html=True)

st.markdown('<h3 class="report-title">Category Sales Count Report</h3>', unsafe_allow_html=True)

# Get report data for selected date and location
if selected_date is not None and selected_location is not None:
    try:
        # Get report data from database with the selected interval type
        report_df = utils.get_report_data(selected_date, selected_location, interval_type=selected_interval)
    except Exception as e:
        st.error(f"Error retrieving report data: {str(e)}")
        report_df = pd.DataFrame()
else:
    report_df = pd.DataFrame()

# Format data for display
if not report_df.empty:
    # Ensure all numeric columns are integers
    numeric_cols = ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 'Corn', 'Grits', 'Pots', 'Total']
    report_df[numeric_cols] = report_df[numeric_cols].fillna(0).astype(int)

    # Add service totals
    service_totals = []
    for service in ['Lunch', 'Dinner']:
        service_data = report_df[report_df['Service'] == service]
        if not service_data.empty:
            service_total = service_data[numeric_cols].sum()
            service_total['Service'] = f'{service} Total'
            service_total['Interval'] = ''
            service_totals.append(service_total)

    # Add grand total
    grand_total = report_df[numeric_cols].sum()
    grand_total['Service'] = 'Grand Total'
    grand_total['Interval'] = ''

    # Combine all rows
    report_df = pd.concat([
        report_df,
        pd.DataFrame(service_totals),
        pd.DataFrame([grand_total])
    ]).fillna('')

# Add a sort order column to maintain totals at the bottom when sorting
if not report_df.empty:
    # Create a sort helper column (hidden)
    report_df['_sort_order'] = 0  # Default value for regular rows
    
    # Mark service totals and grand total with higher values to ensure they stay at the bottom
    report_df.loc[report_df['Service'].str.contains('Total', case=False, na=False), '_sort_order'] = 1  # Service totals
    report_df.loc[report_df['Service'] == 'Grand Total', '_sort_order'] = 2  # Grand total
    
    # Sort by sort_order first, then by service and interval
    report_df = report_df.sort_values(['_sort_order', 'Service', 'Interval'])

# Filter columns to exclude the sort order helper column
display_columns = [col for col in report_df.columns if col != '_sort_order'] if not report_df.empty else []

# Since Streamlit doesn't support disabling sorting, we'll use a static table instead of dataframe
# Define the custom CSS to style the table
st.markdown("""
<style>
    .report-table {
        width: 100%;
        border-collapse: collapse;
        text-align: center;
    }
    .report-table th {
        background-color: #f0f2f6;
        border: 1px solid #ddd;
        padding: 8px;
        text-align: center;
        font-weight: bold;
    }
    .report-table td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: center;
    }
    .report-table tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    .total-row {
        font-weight: bold;
        background-color: #e6e6e6 !important;
    }
    .grand-total-row {
        font-weight: bold;
        background-color: #d9d9d9 !important;
    }
</style>
""", unsafe_allow_html=True)

# Generate HTML for the table if there's data
if not report_df.empty:
    # Start table
    table_html = "<table class='report-table'>"
    
    # Table header
    table_html += "<tr>"
    for col in display_columns:
        header_name = 'Time' if col == 'Interval' else col
        table_html += f"<th>{header_name}</th>"
    table_html += "</tr>"
    
    # Process data by service period to maintain order within each service
    for service in ['Lunch', 'Dinner']:
        # Get regular rows for this service (excluding totals)
        service_rows = report_df[(report_df['Service'] == service) & 
                                (~report_df['Service'].str.contains('Total', case=False, na=False))]
        
        # Sort by time within the service period
        service_rows = service_rows.sort_values('Interval')
        
        # Display regular time interval rows
        for _, row in service_rows.iterrows():
            # Add row
            table_html += f"<tr class=''>"
            for col in display_columns:
                # Format numeric values
                if col in ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 'Corn', 'Grits', 'Pots', 'Total']:
                    table_html += f"<td>{int(row[col]) if row[col] != '' else ''}</td>"
                else:
                    table_html += f"<td>{row[col]}</td>"
            table_html += "</tr>"
        
        # Add service total row
        service_total_row = report_df[report_df['Service'] == f'{service} Total']
        if not service_total_row.empty:
            for _, row in service_total_row.iterrows():
                table_html += f"<tr class='total-row'>"
                for col in display_columns:
                    # Format numeric values
                    if col in ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 'Corn', 'Grits', 'Pots', 'Total']:
                        table_html += f"<td>{int(row[col]) if row[col] != '' else ''}</td>"
                    else:
                        table_html += f"<td>{row[col]}</td>"
                table_html += "</tr>"
    
    # Add grand total row at the very end
    grand_total_row = report_df[report_df['Service'] == 'Grand Total']
    if not grand_total_row.empty:
        for _, row in grand_total_row.iterrows():
            table_html += f"<tr class='grand-total-row'>"
            for col in display_columns:
                # Format numeric values
                if col in ['1/2 Chix', '1/2 Ribs', 'Full Ribs', '6oz Mod', '8oz Mod', 'Corn', 'Grits', 'Pots', 'Total']:
                    table_html += f"<td>{int(row[col]) if row[col] != '' else ''}</td>"
                else:
                    table_html += f"<td>{row[col]}</td>"
            table_html += "</tr>"
    
    # End table
    table_html += "</table>"
    
    # Display the HTML table
    st.markdown(table_html, unsafe_allow_html=True)
else:
    st.info("No data available for the selected date and location.")