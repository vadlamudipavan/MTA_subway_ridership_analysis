import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import traceback # Import traceback for detailed error printing

# --- Database Connection Details ---
# IMPORTANT: Replace these with your actual PostgreSQL credentials.
# You can also set these as environment variables for production.
DB_USER = os.getenv('DB_USER', 'subway_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '12345678')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'nyc_subway_db')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Streamlit App Configuration ---
st.set_page_config(layout="wide", page_title="MTA Subway Ridership Dashboard")

st.title("MTA Subway Ridership Analysis & Forecast")
st.markdown("---")

# Initialize DataFrames outside any blocks to ensure they're always defined
df_historical = pd.DataFrame()
df_forecast = pd.DataFrame()
df_station_map = pd.DataFrame() # Also initialize this here

# --- Function Definitions (Moved here, outside of try/except blocks) ---
# These functions should always be defined, regardless of DB connection success

@st.cache_data # Cache data to improve performance for subsequent runs
def load_data(table_name, _engine_instance): # _engine_instance tells Streamlit not to hash this argument
    """
    Loads data from the specified PostgreSQL table.
    Aggregates 'hourly_ridership' to daily totals for performance.
    """
    try:
        print(f"Attempting to load data from table: {table_name}")
        
        if table_name == 'hourly_ridership':
            # Load aggregated daily historical data directly from DB
            query = """
            SELECT
                DATE_TRUNC('day', transit_timestamp) AS transit_timestamp,
                SUM("hourly_ridership_total") AS ridership -- Sum actual ridership column
            FROM hourly_ridership
            GROUP BY 1
            ORDER BY 1;
            """
            df = pd.read_sql_query(query, _engine_instance) # Use the argument with underscore
            print(f"Successfully loaded {len(df)} daily aggregated rows from {table_name}.")
        else:
            # For forecasts or other tables, load directly (assuming they are smaller)
            df = pd.read_sql_table(table_name, _engine_instance) # Use the argument with underscore
            print(f"Successfully loaded {len(df)} rows from {table_name}.")

        # Convert relevant columns to datetime objects
        if 'transit_timestamp' in df.columns:
            df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'])
        if 'forecast_timestamp' in df.columns:
            df['forecast_timestamp'] = pd.to_datetime(df['forecast_timestamp'])
        return df
    except Exception as e:
        st.error(f"Error loading data from {table_name}: {e}")
        print(f"ERROR: Failed to load data from {table_name}. Details: {e}") # Debug print to terminal
        traceback.print_exc() # Print full traceback to terminal
        return pd.DataFrame() # Return empty DataFrame on error

@st.cache_data # Cache data for the station map
def load_station_ridership_for_map(_engine_instance): # _engine_instance tells Streamlit not to hash this argument
    """Loads aggregated historical ridership by station for the map visualization."""
    query = """
    SELECT
        station_name,
        latitude,
        longitude,
        SUM("hourly_ridership_total") AS total_ridership -- Summing the actual column name
    FROM hourly_ridership
    GROUP BY station_name, latitude, longitude
    ORDER BY total_ridership DESC;
    """
    try:
        df_station_ridership = pd.read_sql_query(query, _engine_instance) # Use the argument with underscore
        print(f"Loaded {len(df_station_ridership)} unique stations for map.") # Debug print
        return df_station_ridership
    except Exception as e:
        st.error(f"Error loading station ridership for map: {e}")
        print(f"ERROR: Failed to load station ridership for map. Details: {e}") # Debug print
        traceback.print_exc()
        return pd.DataFrame()

# --- Database Connection and Data Loading (Only the actual calls are in try block) ---
# Initialize engine to None for safety if creation fails
engine = None 

try:
    print("Attempting to create database engine...")
    engine = create_engine(DATABASE_URL)
    print("Database engine created.")

    with st.spinner("Loading historical and forecast data from database..."):
        # Pass the created engine instance to the functions
        df_historical = load_data('hourly_ridership', engine) 
        st.write("Historical data loaded status:", "Empty" if df_historical.empty else "Loaded with data")
        
        df_forecast = load_data('hourly_ridership_forecasts', engine)
        st.write("Forecast data loaded status:", "Empty" if df_forecast.empty else "Loaded with data")

        # Load data for the station map only if engine is valid
        if engine: # Only attempt if engine was created successfully
            df_station_map = load_station_ridership_for_map(engine)
            st.write("Station map data loaded status:", "Empty" if df_station_map.empty else "Loaded with data")

except Exception as e:
    st.error(f"A critical error occurred during database connection or initial data loading: {e}")
    print(f"CRITICAL ERROR: {e}")
    traceback.print_exc()

st.markdown("---")

## Data Overviews
if not df_historical.empty:
    st.header("Historical Ridership Data Overview")
    st.write(f"Total historical records (daily aggregated): {len(df_historical):,}")
    st.write(df_historical.head())
else:
    st.warning("Historical data could not be loaded. Please check database connection and table.")

if not df_forecast.empty:
    st.header("Forecasted Ridership Data Overview")
    st.write(f"Total forecast records (hourly): {len(df_forecast):,}")
    st.write(df_forecast.head())
else:
    st.warning("Forecast data could not be loaded. Please run the forecasting script first.")

st.markdown("---")

## Ridership Trends and Forecast
st.header("Daily Ridership Trends and Forecast")

if not df_historical.empty and not df_forecast.empty:
    # df_historical is already daily aggregated from the database query
    df_historical_plot = df_historical.copy()
    df_historical_plot.rename(columns={'transit_timestamp': 'ds', 'ridership': 'y'}, inplace=True)

    # Prepare forecast data for plotting (yhat as 'y')
    df_forecast_plot = df_forecast.copy()
    df_forecast_plot.rename(columns={'forecast_timestamp': 'ds', 'yhat': 'y'}, inplace=True)
    
    # Concatenate historical and forecast data for plotting
    last_historical_date = df_historical_plot['ds'].max()
    # Filter forecast to start from the day after the last historical date
    # Ensure forecast data doesn't overlap with historical data's last day
    df_forecast_plot = df_forecast_plot[df_forecast_plot['ds'] > last_historical_date]


    # Create a combined DataFrame for Plotly
    combined_df = pd.concat([
        df_historical_plot.assign(Type='Historical'),
        df_forecast_plot.assign(Type='Forecast')
    ], ignore_index=True)

    fig = px.line(combined_df, x='ds', y='y', color='Type',
                  title='Daily Total Ridership: Historical vs. Forecast',
                  labels={'y': 'Total Ridership', 'ds': 'Date'},
                  hover_data={'y': ':.0f', 'ds': '|%Y-%m-%d'})
    
    if last_historical_date is not pd.NaT:
        # Convert Timestamp to Unix milliseconds for plotly.add_vline
        last_historical_date_ms = last_historical_date.timestamp() * 1000
        
        fig.add_vline(x=last_historical_date_ms, line_width=2, line_dash="dash", line_color="gray",
                      annotation_text="Forecast Start", annotation_position="top right")

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Explore Raw Forecast Details")
    st.write("Below is a table of the raw hourly forecast data loaded from the database.")
    st.dataframe(df_forecast)
    
else:
    st.info("Visualizations will appear once both historical and forecast data are loaded. Check above warnings/errors.")

st.markdown("---")

## Station-Level Ridership Overview
st.header("Historical Ridership by Subway Station")

# df_station_map is loaded within the initial try-except block
if not df_station_map.empty: # This condition now checks if df_station_map was successfully loaded
    # Get coordinates for NYC or a central point for initial map view
    center_lat = df_station_map['latitude'].mean() if not df_station_map.empty else 40.75
    center_lon = df_station_map['longitude'].mean() if not df_station_map.empty else -74.00

    fig_map = px.scatter_mapbox(df_station_map,
                                lat="latitude",
                                lon="longitude",
                                size="total_ridership", # Marker size based on total ridership
                                color="total_ridership", # Color based on total ridership
                                hover_name="station_name",
                                hover_data={"total_ridership": ":,.0f"},
                                color_continuous_scale=px.colors.sequential.Plasma,
                                zoom=10, # Adjust zoom level as needed
                                center={"lat": center_lat, "lon": center_lon},
                                title="Total Historical Ridership by Subway Station",
                                mapbox_style="carto-positron") # Or "open-street-map", "stamen-terrain", "stamen-watercolor"

    fig_map.update_layout(margin={"r":0,"t":50,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

    st.subheader("Top 10 Busiest Stations")
    st.dataframe(df_station_map.sort_values(by="total_ridership", ascending=False).head(10))
else:
    st.warning("Could not load station ridership data for the map. Check database connection and table.")

st.markdown("---")
st.caption("Powered by Streamlit, Pandas, Plotly, and PostgreSQL")