# scripts/data_cleaning.py

import pandas as pd
import os
from datetime import datetime, timedelta

def load_and_clean_hourly_ridership_data(raw_file_path: str = 'mta_hourly_ridership_first_8000000.csv', 
                                         processed_output_path: str = 'data/processed/mta_hourly_ridership_cleaned.csv') -> pd.DataFrame:
    """
    Loads the MTA Subway Hourly Ridership data, cleans it, and performs feature engineering.

    Args:
        raw_file_path (str): Path to the raw hourly ridership CSV file.
        processed_output_path (str): Path to save the cleaned and processed CSV file.

    Returns:
        pd.DataFrame: Cleaned and processed DataFrame.
    """
    if not os.path.exists(raw_file_path):
        print(f"Error: Raw data file not found at {raw_file_path}. Please run data_ingestion.py first.")
        return pd.DataFrame()

    print(f"Loading raw hourly ridership data from: {raw_file_path} (this might take a moment for 833MB)...")
    try:
        df = pd.read_csv(raw_file_path, low_memory=False) 
        print(f"Raw data loaded. Shape: {df.shape}")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return pd.DataFrame()

    # --- Initial Cleaning and Type Conversion ---
    print("Starting initial cleaning and feature engineering...")

    # 1. Convert 'transit_timestamp' to datetime objects
    df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'], errors='coerce')
    df.dropna(subset=['transit_timestamp'], inplace=True)

    # 2. Rename columns for clarity
    df.rename(columns={
        'station_complex_id': 'station_id',
        'station_complex': 'station_name',
        'ridership': 'hourly_ridership_total'
    }, inplace=True)

    # --- Handle non-numeric 'station_id' values (like 'TRAM1') ---
    # Convert 'station_id' to numeric, coercing errors to NaN.
    # This will turn 'TRAM1' into NaN.
    initial_rows = df.shape[0]
    df['station_id'] = pd.to_numeric(df['station_id'], errors='coerce')
    
    # Identify and optionally inspect rows with NaN station_id (i.e., the TRAM1 rows)
    nan_station_ids = df[df['station_id'].isna()]
    if not nan_station_ids.empty:
        print(f"Found {len(nan_station_ids)} rows with non-numeric station_id (e.g., 'TRAM1').")
        print("These rows will be dropped to ensure station_id is numeric for analysis.")
        # print("Example of non-numeric station_id rows:")
        # print(nan_station_ids.head())
        df.dropna(subset=['station_id'], inplace=True)
        print(f"Dropped {initial_rows - df.shape[0]} rows. Remaining rows: {df.shape[0]}")
    
    # Now that non-numeric values are removed, convert to integer type
    df['station_id'] = df['station_id'].astype('int32')


    # 3. Handle potential non-numeric ridership
    df['hourly_ridership_total'] = pd.to_numeric(df['hourly_ridership_total'], errors='coerce').fillna(0)
    df['hourly_ridership_total'] = df['hourly_ridership_total'].apply(lambda x: max(0, x))

    # --- Feature Engineering ---
    df['date'] = df['transit_timestamp'].dt.date
    df['hour'] = df['transit_timestamp'].dt.hour
    df['day_of_week_num'] = df['transit_timestamp'].dt.weekday # Monday=0, Sunday=6
    df['day_of_week_name'] = df['transit_timestamp'].dt.day_name()
    df['month'] = df['transit_timestamp'].dt.month
    df['month_name'] = df['transit_timestamp'].dt.month_name()
    df['year'] = df['transit_timestamp'].dt.year
    df['is_weekend'] = df['day_of_week_num'].isin([5, 6]) # Saturday and Sunday

    # Define rush hours (e.g., 6-9 AM and 4-7 PM)
    df['is_am_rush'] = df['hour'].isin([6, 7, 8, 9])
    df['is_pm_rush'] = df['hour'].isin([16, 17, 18, 19])
    
    # 4. Data Type Optimization
    df['hourly_ridership_total'] = df['hourly_ridership_total'].astype('int32')
    df['hour'] = df['hour'].astype('int8')
    df['day_of_week_num'] = df['day_of_week_num'].astype('int8')
    df['month'] = df['month'].astype('int8')
    df['year'] = df['year'].astype('int16')
    df['is_weekend'] = df['is_weekend'].astype('bool')
    df['is_am_rush'] = df['is_am_rush'].astype('bool')
    df['is_pm_rush'] = df['is_pm_rush'].astype('bool')
    
    for col in ['station_name', 'borough', 'day_of_week_name', 'month_name']:
        if col in df.columns:
            df[col] = df[col].astype('category')

    print(f"Cleaning and feature engineering complete. Final shape: {df.shape}")
    print("\n--- Cleaned DataFrame Info ---")
    df.info()
    print("\n--- Head of Cleaned DataFrame ---")
    print(df.head())
    
    os.makedirs(os.path.dirname(processed_output_path), exist_ok=True)
    df.to_csv(processed_output_path, index=False)
    print(f"\nCleaned and processed data saved to: {processed_output_path}")

    return df

if __name__ == "__main__":
    raw_file = 'data/raw/mta_hourly_ridership_first_8000000.csv' # New filename 
    processed_file = 'data/processed/mta_hourly_ridership_cleaned.csv'
    
    cleaned_df = load_and_clean_hourly_ridership_data(raw_file, processed_file)
    
    if not cleaned_df.empty:
        print(f"\nNumber of unique station complexes: {cleaned_df['station_name'].nunique()}")
        print(f"Data date range: {cleaned_df['transit_timestamp'].min()} to {cleaned_df['transit_timestamp'].max()}")