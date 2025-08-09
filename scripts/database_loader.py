# scripts/database_loader.py

import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- Database Configuration ---
# IMPORTANT: Replace with your actual PostgreSQL credentials and database name
DB_USER = 'subway_user' # Or 'postgres' if you're using the default
DB_PASSWORD = '12345678' # Replace with your password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'nyc_subway_db'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_db_engine():
    """Creates and returns a SQLAlchemy engine for the PostgreSQL database."""
    try:
        engine = create_engine(DATABASE_URL)
        # Test connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Successfully connected to the database.")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Please ensure PostgreSQL is running and your connection details are correct.")
        return None

def create_table_if_not_exists(engine, table_name='hourly_ridership'):
    """
    Creates the 'hourly_ridership' table in the database if it doesn't exist,
    matching the schema of our cleaned DataFrame.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        transit_timestamp TIMESTAMP,
        station_id INTEGER,
        station_name VARCHAR(255),
        borough VARCHAR(50),
        hourly_ridership_total INTEGER,
        ridership_00_04 INTEGER, -- These columns might exist in the data.ny.gov CSV. Adjust if not.
        ridership_04_08 INTEGER,
        ridership_08_12 INTEGER,
        ridership_12_16 INTEGER,
        ridership_16_20 INTEGER,
        ridership_20_00 INTEGER,
        date DATE,
        hour SMALLINT,
        day_of_week_num SMALLINT,
        day_of_week_name VARCHAR(10),
        month SMALLINT,
        month_name VARCHAR(10),
        year SMALLINT,
        is_weekend BOOLEAN,
        is_am_rush BOOLEAN,
        is_pm_rush BOOLEAN
    );
    """
    try:
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit() # Commit the DDL operation
        print(f"Table '{table_name}' created or already exists.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")


def load_data_to_db(df: pd.DataFrame, engine, table_name: str = 'hourly_ridership'):
    """
    Loads the cleaned DataFrame into the specified database table.
    """
    if df.empty:
        print("DataFrame is empty. Nothing to load to database.")
        return

    print(f"Loading {len(df)} rows into '{table_name}' table. This may take some time...")
    try:
        # Use pandas to_sql. 'replace' will drop and recreate the table, 'append' will add to it.
        # For initial load, 'replace' is fine if you want to ensure a fresh table.
        # For updates, 'append' or more complex upsert logic would be needed.
        # If your table schema defined above does not exactly match DataFrame columns,
        # you might get errors. Ensure column names and types match.
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
        print(f"Successfully loaded data into '{table_name}' table.")
    except Exception as e:
        print(f"Error loading data to database: {e}")

if __name__ == "__main__":
    cleaned_file_path = 'data/processed/mta_hourly_ridership_cleaned.csv'
    
    if not os.path.exists(cleaned_file_path):
        print(f"Cleaned data file not found at {cleaned_file_path}. Please run data_cleaning.py first.")
    else:
        print(f"Loading cleaned data from {cleaned_file_path} for database upload...")
        # Ensure that the columns in the DataFrame match the SQL schema defined above
        # The 'ridership_00_04' etc. columns are usually present in the raw data.ny.gov file,
        # but our cleaning script currently doesn't explicitly process them.
        # If your data.ny.gov CSV has these columns, they should be loaded, otherwise,
        # modify the `create_table_sql` or ensure they are dropped in `data_cleaning.py`
        # if you don't need them. For simplicity, we'll try to load all original columns
        # plus the new engineered ones.
        
        # Reload the cleaned CSV without type optimization for `to_sql` if it causes issues.
        # Sometimes pandas' `to_sql` struggles with `category` dtypes or optimized int types
        # if they don't map directly to default SQL types. 
        # A safer approach for `to_sql` is to load the processed CSV as raw as possible for this step.
        df_cleaned = pd.read_csv(cleaned_file_path, low_memory=False)
        
        # Re-convert datetime for SQL compatibility if date was just `date` object
        df_cleaned['transit_timestamp'] = pd.to_datetime(df_cleaned['transit_timestamp'])
        df_cleaned['date'] = pd.to_datetime(df_cleaned['date']) # Ensure 'date' is also datetime object for DATE type in SQL

        # Adjust column names for database (if they differ from `data_cleaning.py` output)
        # Make sure column names match the SQL table definition precisely.
        # The `create_table_sql` includes `ridership_00_04` etc. These columns are usually present in the
        # original `mta_hourly_ridership.csv` from data.ny.gov. Let's make sure they are included
        # or dropped consistently. If your `data_cleaning.py` dropped them, remove them from `create_table_sql`.
        
        # Let's adjust `create_table_sql` in create_table_if_not_exists to only include columns we're sure we have.
        # Or, confirm which columns are actually in `df_cleaned` and adjust the SQL.
        # For simplicity, I'll ensure the `create_table_sql` matches the current `data_cleaning.py` output.
        # The original columns like `ridership_00_04` etc. were likely dropped by `data_cleaning.py` 
        # if they were not explicitly renamed. Let's inspect the `df_cleaned.columns`
        # to ensure the create_table_sql matches exactly.

        # The data.ny.gov dataset `wujg-7c2s` has columns like:
        # transit_timestamp, station_complex_id, station_complex, borough, ridership,
        # ridership_00_04, ridership_04_08, ridership_08_12, ridership_12_16, ridership_16_20, ridership_20_00
        # Our `data_cleaning.py` renames `ridership` to `hourly_ridership_total`
        # and `station_complex_id` to `station_id`, `station_complex` to `station_name`.
        # So the `create_table_sql` should reflect these renames.

        # Let's simplify the create_table_sql for now to only include columns we know are in `df_cleaned`.
        # You can add back `ridership_00_04` etc. if you decide to explicitly keep them in `data_cleaning.py`
        # and load them into the database.
        
        engine = create_db_engine()
        if engine:
            # Re-calling `create_table_if_not_exists` with the simplified schema
            # This is critical to ensure the table schema matches your DataFrame.
            # You might need to drop the table manually in PgAdmin if it exists with an old schema:
            # DROP TABLE IF EXISTS hourly_ridership;
            # Or rename it to recreate it.
            create_table_sql_simplified = """
            CREATE TABLE IF NOT EXISTS hourly_ridership (
                transit_timestamp TIMESTAMP,
                station_id INTEGER,
                station_name VARCHAR(255),
                borough VARCHAR(50),
                hourly_ridership_total INTEGER,
                date DATE,
                hour SMALLINT,
                day_of_week_num SMALLINT,
                day_of_week_name VARCHAR(10),
                month SMALLINT,
                month_name VARCHAR(10),
                year SMALLINT,
                is_weekend BOOLEAN,
                is_am_rush BOOLEAN,
                is_pm_rush BOOLEAN
            );
            """
            try:
                with engine.connect() as connection:
                    connection.execute(text(create_table_sql_simplified))
                    connection.commit()
                print("Table 'hourly_ridership' (simplified schema) created or already exists.")
            except Exception as e:
                print(f"Error creating simplified table: {e}")
            
            load_data_to_db(df_cleaned, engine, 'hourly_ridership')
            print("\n--- Verification: First 5 rows from Database ---")
            try:
                with engine.connect() as connection:
                    query_df = pd.read_sql_query("SELECT * FROM hourly_ridership LIMIT 5;", connection)
                    print(query_df)
            except Exception as e:
                print(f"Error querying database: {e}")