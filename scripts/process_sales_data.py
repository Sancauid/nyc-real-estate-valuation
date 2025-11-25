import pandas as pd
import os
import re

# --- CONFIGURATION ---

# Define the standard, clean column names we want in our final DataFrame.
CANONICAL_COLUMNS = [
    'borough', 'neighborhood', 'building_class_category', 'tax_class_at_present',
    'block', 'lot', 'easement', 'building_class_at_present', 'address',
    'apartment_number', 'zip_code', 'residential_units', 'commercial_units',
    'total_units', 'land_square_feet', 'gross_square_feet', 'year_built',
    'tax_class_at_time_of_sale', 'building_class_at_time_of_sale',
    'sale_price', 'sale_date'
]

# This is the core of the script's robustness.
# It maps all known messy variations of column names to our clean, canonical names.
# Add any new variations you find here.
COLUMN_MAP = {
    'borough': 'borough',
    'neighborhood': 'neighborhood',
    'building class category': 'building_class_category',
    'tax class as of final roll 18/19': 'tax_class_at_present',
    'tax class at present': 'tax_class_at_present',
    'block': 'block',
    'lot': 'lot',
    'ease-ment': 'easement', # Handles the typo in some files
    'building class as of final roll 18/19': 'building_class_at_present',
    'building class at present': 'building_class_at_present',
    'address': 'address',
    'apartment number': 'apartment_number',
    'zip code': 'zip_code',
    'residential units': 'residential_units',
    'commercial units': 'commercial_units',
    'total units': 'total_units',
    'land square feet': 'land_square_feet',
    'gross square feet': 'gross_square_feet',
    'year built': 'year_built',
    'tax class at time of sale': 'tax_class_at_time_of_sale',
    'building class at time of sale': 'building_class_at_time_of_sale',
    'sale price': 'sale_price',
    'sale date': 'sale_date'
}


def find_header_row(file_path, keyword='BOROUGH'):
    """
    Dynamically finds the header row in an Excel file by searching for a keyword.
    
    Args:
        file_path (str): The path to the Excel file.
        keyword (str): The keyword to identify the header row (case-insensitive).
        
    Returns:
        int: The index of the header row (0-based).
    
    Raises:
        ValueError: If the keyword is not found in the first 10 rows.
    """
    df_preview = pd.read_excel(file_path, header=None, nrows=10)
    for i, row in df_preview.iterrows():
        # Check if any cell in the row contains the keyword, ignoring case and whitespace
        if any(str(cell).strip().upper() == keyword for cell in row):
            return i
    raise ValueError(f"Header keyword '{keyword}' not found in the first 10 rows of {file_path}")


def clean_column_names(df, column_map):
    """
    Standardizes the column names of a DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame with messy columns.
        column_map (dict): A dictionary mapping messy names to clean names.
        
    Returns:
        pd.DataFrame: A DataFrame with standardized column names.
    """
    # Create a cleaning function for column names
    def clean_name(col):
        # Remove newlines, leading/trailing spaces, and convert to lowercase
        cleaned = re.sub(r'\s+', ' ', str(col)).strip().lower()
        return cleaned

    # Apply the cleaning function and then the mapping
    df.columns = [clean_name(col) for col in df.columns]
    df = df.rename(columns=column_map)
    
    return df


def process_sales_file(file_path):
    """
    Loads a single NYC sales Excel file, cleans it, and standardizes it.
    
    Args:
        file_path (str): The path to the Excel file.
        
    Returns:
        pd.DataFrame: A cleaned and standardized DataFrame for one file.
    """
    print(f"Processing file: {os.path.basename(file_path)}...")
    
    # 1. Dynamically find the header row
    try:
        header_row_index = find_header_row(file_path)
    except ValueError as e:
        print(f"Could not process {file_path}: {e}")
        return None
        
    # 2. Load the data using the correct header row
    df = pd.read_excel(file_path, skiprows=header_row_index)
    
    # 3. Clean and standardize column names
    df = clean_column_names(df, COLUMN_MAP)
    
    # 4. Ensure all canonical columns exist, adding missing ones with NaN
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    
    # Keep only the columns we defined as standard
    df = df[CANONICAL_COLUMNS]
    
    # 5. Convert data types with robust error handling
    numeric_cols = [
        'sale_price', 'gross_square_feet', 'land_square_feet', 'residential_units',
        'commercial_units', 'total_units', 'year_built', 'zip_code', 'block', 'lot'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    
    # 6. Drop rows that are completely empty
    df.dropna(how='all', inplace=True)
    
    print(f"-> Found {len(df)} rows.")
    return df


def main():
    """
    Main function to orchestrate the loading and cleaning of all sales files.
    """
    # Adjust paths to match your project structure
    # This script assumes it is in a 'scripts' folder, and data is in '../data/raw'
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_path = os.path.join(project_root, 'data', 'raw')
    processed_data_path = os.path.join(project_root, 'data', 'processed')
    output_file = os.path.join(processed_data_path, 'nyc_sales_combined.parquet')

    # Ensure the output directory exists
    os.makedirs(processed_data_path, exist_ok=True)

    # Find all Excel files in the raw data directory
    all_files = [os.path.join(raw_data_path, f) for f in os.listdir(raw_data_path) 
                 if f.endswith('.xlsx') and not f.startswith('~')]

    if not all_files:
        print(f"No Excel files found in {raw_data_path}. Please add your data files.")
        return

    # Process each file and store the resulting DataFrame in a list
    all_dfs = [process_sales_file(f) for f in all_files]
    
    # Filter out any files that failed to process (returned None)
    all_dfs = [df for df in all_dfs if df is not None]

    if not all_dfs:
        print("No data was successfully processed.")
        return

    # Combine all DataFrames into one
    print("\nCombining all processed files...")
    master_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total combined rows: {len(master_df)}")
    master_df['tax_class_at_present'] = master_df['tax_class_at_present'].astype(str)
    master_df['apartment_number'] = master_df['apartment_number'].astype(str)
    
    # Save the final, clean DataFrame to a Parquet file for efficient use later
    print(f"Saving combined data to {output_file}...")
    master_df.to_parquet(output_file)
    
    print("\nProcessing complete! Your clean data is ready.")
    print("Next steps: Further cleaning, filtering, and feature engineering.")


if __name__ == "__main__":
    main()