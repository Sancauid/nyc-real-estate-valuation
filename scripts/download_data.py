import os
import requests
from time import sleep

# --- Configuration ---
YEARS = range(2018, 2025) # This will go from 2018 up to and including 2024
BOROUGHS = ['manhattan', 'bronx', 'brooklyn', 'queens', 'staten_island']
BASE_URL = "https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/"

# --- Output Directory ---
# Assumes you run this script from the root of your project folder (e.g., 'real-estate-valuation/')
OUTPUT_DIR = os.path.join('data', 'raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Files will be saved in: {os.path.abspath(OUTPUT_DIR)}")

# --- Download Logic ---
def download_file(url, filepath):
    """Downloads a file from a URL to a specified path."""
    try:
        response = requests.get(url, timeout=30) # 30-second timeout
        # Check if the request was successful
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            print(f"  SUCCESS: Saved to {os.path.basename(filepath)}")
            return True
        else:
            print(f"  FAILED: Status code {response.status_code} for {url}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"  FAILED: An error occurred: {e}")
        return False

# --- Main Loop ---
if __name__ == "__main__":
    total_files = len(YEARS) * len(BOROUGHS)
    downloaded_count = 0
    
    print("\nStarting download of NYC Property Sales data...")
    print("-" * 50)
    
    for year in YEARS:
        for borough in BOROUGHS:
            # Construct the URL based on the observed pattern
            # Note: Staten Island uses an underscore in the filename
            file_borough_name = borough.replace('_', '-') # URL path uses hyphen
            if borough == 'staten_island':
                file_borough_name = 'staten-island'

            filename = f"{year}_{borough}.xlsx"
            file_url = f"{BASE_URL}{year}/{filename}"
            
            # Construct the full local path to save the file
            local_filepath = os.path.join(OUTPUT_DIR, filename)
            
            print(f"Downloading {filename}...")
            
            if download_file(file_url, local_filepath):
                downloaded_count += 1
            
            # Be a good citizen and don't spam the server
            sleep(1) # Wait 1 second between requests

    print("-" * 50)
    print(f"Download complete. {downloaded_count}/{total_files} files were successfully downloaded.")