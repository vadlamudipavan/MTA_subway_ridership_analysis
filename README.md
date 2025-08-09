# MTA Subway Ridership Analysis and Forecast

## Project Overview

This project delivers an end-to-end data solution for analyzing historical MTA subway ridership and forecasting future trends. Leveraging a robust data engineering pipeline, advanced time series modeling, and an interactive web dashboard, this tool provides critical insights into New York City's public transit system. It's designed to support strategic planning, resource allocation, and operational adjustments for the MTA or interested stakeholders.

**Key Features:**
* **Automated Data Engineering:** Fetches raw hourly ridership data, performs comprehensive cleaning and transformation, and loads it into a PostgreSQL database.
* **Time Series Forecasting:** Utilizes the **Prophet** library (from Meta, formerly Facebook) to build a robust forecasting model that captures complex patterns, including daily, weekly, and yearly seasonality, and holiday effects, to predict future ridership.
* **Interactive Web Dashboard (Streamlit):** Provides a user-friendly interface to visualize historical ridership trends, explore future forecasts, and analyze station-level activity through interactive charts and maps.
* **Database-Optimized Performance:** Aggregates large historical datasets directly within PostgreSQL, ensuring efficient data retrieval and smooth dashboard performance, even with millions of records.
* **Geospatial Analysis:** Incorporates an interactive map to display total historical ridership for each subway station, revealing geographical patterns and highlighting key transit hubs.

## Technologies Used

* **Python 3.x:** Core programming language
    * `pandas`: For powerful data manipulation and analysis.
    * `streamlit`: To build the interactive web application and dashboard.
    * `plotly`: For creating stunning, interactive statistical charts and geospatial maps.
    * `sqlalchemy`, `psycopg2`: For seamless connection and interaction with the PostgreSQL database.
    * `prophet`: The time series forecasting library (ensure you install `prophet` or `fbprophet` as used in your `forecast_model.py`).
    * `requests`, `io`, `zipfile`: Utilized in `data_cleaning.py` for fetching and extracting data from web sources.
* **PostgreSQL:** A powerful, open-source relational database used to store cleaned and processed ridership data efficiently.
* **Git/GitHub:** (Highly recommended for version control and collaborative development)

## Project Structure
nyc_project/
├── .venv/                         # Python virtual environment (auto-generated)
├── app.py                         # Streamlit web application (dashboard)
├── data_cleaning.py               # Script for data fetching, cleaning, and DB loading
├── forecast_model.py              # Script for training the Prophet model and generating forecasts
└── README.md                      # This documentation file
## Setup and Installation

Follow these steps to set up and run the project locally:

### 1. Clone the Repository (Optional, if using Git)

If your project is hosted on GitHub:
```bash
git clone [LINK TO YOUR GITHUB REPOSITORY]
cd nyc_project

2. Create and Activate a Python Virtual Environment
It's crucial to use a virtual environment to manage project dependencies:

python -m venv .venv
# On Windows:
.\.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

3. Install Required Python Dependencies
With your virtual environment active, install all necessary libraries:

pip install pandas streamlit plotly "sqlalchemy[psycopg2]" prophet requests

4. Set Up PostgreSQL Database
Before running the scripts, ensure you have PostgreSQL installed and running on your system.

Create a new database for this project, for example, named mta_ridership_db. You can do this using psql (the PostgreSQL command-line client) or a GUI tool like pgAdmin:
CREATE DATABASE mta_ridership_db;

Configure Database Credentials: Open data_cleaning.py and app.py. Locate the DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and DB_NAME variables at the top of these files. Replace the placeholder values with your actual PostgreSQL credentials. For production environments, consider using environment variables for security.

5. Ingest and Clean Data
Run the data_cleaning.py script. This script will download the raw MTA ridership data, process it, and load it into your mta_ridership_db database. This step might take a significant amount of time depending on your internet speed and system performance, as it handles a large dataset.

python data_cleaning.py

6. Generate Ridership Forecasts
After the data is in the database, run the forecast_model.py script. This will train the Prophet model on your historical data and save the generated forecasts back into a new table (hourly_ridership_forecasts) in your database.

python forecast_model.py

7. Launch the Streamlit Dashboard
Finally, start the interactive web dashboard. Ensure your virtual environment is active.

streamlit run app.py

Your Streamlit application will open in your default web browser (typically at http://localhost:8501 or a similar local URL).

Key Insights and Interpretation
This dashboard provides a powerful lens into MTA subway ridership, enabling data-driven decision-making:

Historical Trends
The "Daily Ridership Trends and Forecast" chart visually reveals the historical pulse of NYC's subway system. You can observe:

Strong Seasonality: Distinct weekly patterns (e.g., lower ridership on weekends) and annual cycles (e.g., dips around major holidays or specific seasons).
Long-term Movements: Over extended periods, you might observe overall growth trends, plateaus, or impacts from significant external events (like a post-pandemic recovery).
Forecast Interpretation
The orange "Forecast" line on the main chart provides a data-driven prediction of future daily ridership. This is invaluable for:

Anticipating Demand: Understanding expected future passenger volumes helps in planning train frequencies, staffing levels, and station resource allocation.
Budgeting & Revenue Projection: Forecasted ridership directly impacts farebox revenue estimates.
Strategic Planning: Long-term forecasts can inform decisions on infrastructure upgrades and expansion projects.
Geospatial Insights
The "Historical Ridership by Subway Station" map and "Top 10 Busiest Stations" table offer a geographical perspective:

Busiest Hubs: Easily identify the stations with the highest overall historical ridership (represented by larger and brighter markers on the map). These are critical points for service and infrastructure focus.
Geographical Patterns: Observe if certain boroughs or areas have consistently higher or lower ridership, which can inform targeted marketing, service adjustments, or even urban development strategies.
Resource Allocation: Knowledge of high-traffic stations can guide deployment of staff, cleaning crews, and security personnel.
Future Enhancements
This project serves as a robust foundation. Here are some potential enhancements to further develop its capabilities:

Advanced Dashboard Interactivity: Add filters for station_name, transit_mode, payment_method, or date ranges to the Streamlit app.
External Factor Integration: Incorporate external datasets such as weather, major events (concerts, sports), and holiday schedules into the Prophet model to improve forecast accuracy.
Model Comparison: Experiment with other time series forecasting models (e.g., ARIMA, SARIMAX, Neural Networks) and compare their performance.
Anomaly Detection: Implement real-time anomaly detection to identify unusual ridership spikes or drops that deviate significantly from predicted patterns.
Real-time Data Integration: Explore streaming ridership data (if available) to provide near real-time insights and forecasts.
Deployment: Deploy the Streamlit application to a cloud platform (e.g., Streamlit Community Cloud, AWS, GCP) for wider accessibility.
CI/CD Pipeline: Establish a Continuous Integration/Continuous Deployment pipeline for automated data updates, model retraining, and dashboard deployment.