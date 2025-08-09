# scripts/data_ingestion.py - REVISED TO LIMIT TOTAL ROWS FROM EARLIEST DATE

import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import time
from io import StringIO


def download_data_from_socrata_limited(dataset_id: str, output_path: str, max_rows: int, chunk_size: int = 50000, where_clause: str = None):
    """
    Downloads data from data.ny.gov (Socrata Open Data API) using pagination,
    limiting the total number of rows downloaded.

    Args:
        dataset_id (str): The unique identifier for the dataset on data.ny.gov.
        output_path (str): The local path to save the downloaded CSV.
        max_rows (int): The maximum number of rows to download.
        chunk_size (int): Number of records to fetch per request.
        where_clause (str): A Socrata SoQL WHERE clause to filter data (optional).
    Returns:
        bool: True if download was successful, False otherwise.
    """
    base_url = f"https://data.ny.gov/resource/{dataset_id}.csv"
    
    all_chunks = []
    offset = 0
    total_rows_downloaded = 0

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Starting paginated download for dataset: {dataset_id}, max {max_rows} rows.")

    while total_rows_downloaded < max_rows:
        # Calculate remaining rows needed
        remaining_rows = max_rows - total_rows_downloaded
        current_chunk_limit = min(chunk_size, remaining_rows)

        if current_chunk_limit <= 0:
            print("Reached maximum rows limit. Stopping download.")
            break

        params = {
            '$limit': current_chunk_limit, # Limit each chunk based on remaining rows
            '$offset': offset
        }
        # --- CHANGE HERE: Removed the 'if where_clause:' block ---
        # No 'where_clause' will be added to params, so it downloads from the beginning

        print(f"Fetching chunk from offset {offset}, limit {current_chunk_limit}...")
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            response_content_str: str = response.text 
            chunk_df = pd.read_csv(StringIO(response_content_str), low_memory=False)

            if chunk_df.empty:
                print("No more data to fetch or reached end of available data. Exiting pagination.")
                break

            all_chunks.append(chunk_df)
            total_rows_downloaded += len(chunk_df)
            print(f"Downloaded {len(chunk_df)} rows. Total downloaded: {total_rows_downloaded}")

            # If the number of rows downloaded is less than chunk_size (or current_chunk_limit), we've reached the end of available data
            if len(chunk_df) < current_chunk_limit:
                print("Last available chunk downloaded. Exiting pagination.")
                break

            offset += len(chunk_df) # Increment offset by actual rows downloaded
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error downloading data: {e}")
            return False
        except pd.errors.EmptyDataError:
            print("Received empty data, possibly end of records or error. Exiting pagination.")
            break
        except Exception as e:
            print(f"An unexpected error occurred during download: {e}")
            return False
            
    if not all_chunks:
        print("No data was downloaded.")
        return False

    final_df = pd.concat(all_chunks, ignore_index=True)
    
    # Trim to exactly max_rows if we overshot in the last chunk
    if len(final_df) > max_rows:
        final_df = final_df.head(max_rows)
    
    print(f"All chunks combined. Final total rows: {len(final_df)}")
    
    try:
        final_df.to_csv(output_path, index=False)
        print(f"Successfully saved all downloaded data to: {output_path}")
        return True
    except Exception as e:
        print(f"Error saving combined CSV: {e}")
        return False


if __name__ == "__main__":
    hourly_ridership_dataset_id = "wujg-7c2s"
    max_rows_to_download = 8000000 # Set your desired limit here
    
    # Output filename indicates the limit
    hourly_ridership_output_path = f"data/raw/mta_hourly_ridership_first_{max_rows_to_download}.csv" 

    # --- CHANGE HERE: Removed the 'where_clause_start_date' variable entirely ---
    # This means the 'where_clause' parameter will not be passed to the function,
    # and it will download from the earliest date.

    print(
        f"\n--- Downloading MTA Subway Hourly Ridership Data (first {max_rows_to_download} rows) ---"
    )
    downloaded_limited = download_data_from_socrata_limited(
        dataset_id=hourly_ridership_dataset_id,
        output_path=hourly_ridership_output_path,
        max_rows=max_rows_to_download,
        chunk_size=50000,
        # --- CHANGE HERE: Removed the 'where_clause' argument from the function call ---
        # where_clause=where_clause_start_date, # This line is now gone
    )

    if downloaded_limited:
        print(f"\nSuccessfully downloaded MTA Hourly Ridership data (first {max_rows_to_download} rows).")
    else:
        print(f"\nFailed to download MTA Hourly Ridership data. Check console for errors.")