# Lab 2: Weather Analytics Pipeline (ETL & ELT)

## Project Overview
This project implements an end-to-end data analytics pipeline that fetches historical weather data for London (2023), stores it in **Snowflake**, transforms it using **dbt** (orchestrated by **Airflow**), and visualizes weather trends using **Power BI**.

The goal is to demonstrate an automated workflow combining ETL (Extract, Load) and ELT (Transform) patterns, including **Type 2 Slowly Changing Dimensions (Snapshots)** and **Data Quality Testing**.

## System Architecture
The pipeline consists of the following stages:
1.  **Ingestion (ETL):** Airflow extracts JSON data from the Open-Meteo API and performs a full-refresh load into Snowflake (`RAW` schema).
2.  **Transformation (ELT):** Airflow triggers dbt to:
    * **Run:** Clean and aggregate data (7-day moving averages, rain categorization).
    * **Test:** Validate data integrity (unique keys, non-null values).
    * **Snapshot:** Capture historical data changes (SCD Type 2) for auditing.
3.  **Visualization:** Power BI connects to the analytics tables to display insights.

## Architecture Diagram
<img width="674" height="651" alt="Data_Warehouse_DF drawio" src="https://github.com/user-attachments/assets/796dc444-2839-4c12-b6e9-c1063d9c7cb3" />

## Technologies Used
- **Orchestration:** Apache Airflow (Docker)
- **Data Warehouse:** Snowflake
- **Transformation:** dbt Core (dbt-snowflake adapter)
- **Source:** Open-Meteo Archive API
- **Visualization:** Power BI Desktop
- **Languages:** Python, SQL, YAML

## Project Structure
This repository follows a clean structure separating Airflow DAGs and dbt models.

```text
lab2-weather-analytics-pipeline/
├── dags/
│   ├── open_meteo_weather_etl.py    # DAG 1: ETL (API -> Snowflake RAW)
│   └── dbt_transformation_dag.py    # DAG 2: ELT (Triggers dbt run/test/snapshot)
├── dbt/
│   ├── dbt_project.yml              # dbt configuration
│   ├── profiles.yml                 # Connection profile (uses env_vars for security)
│   ├── snapshots/
│   │   └── weather_snapshot.sql     # Type 2 SCD Snapshot logic
│   └── models/
│       ├── staging/
│       │   ├── sources.yml          # Definition of RAW sources
│       │   └── stg_weather.sql      # Staging view
│       └── marts/
│           ├── schema.yml           # Data quality tests (Unique, Not Null)
│           └── weather_analytics.sql # Final transformation table
├── docker-compose-min.yml           # Airflow configuration with dbt volume mapping
└── README.md                        # Project documentation
```
## Key Technical Features
- **Idempotency:** The ETL DAG uses TRUNCATE + INSERT within a SQL transaction to ensure the pipeline can be re-run without duplicating data.
- **Security:** Snowflake credentials are passed via Airflow Connection variables into dbt using {{ env_var() }}, preventing hardcoded passwords in profiles.yml.
- **Data Quality:** Implemented unique and not_null tests in schema.yml to ensure primary key integrity.
- **Snapshots:** Implemented check strategy snapshots to track changes in weather metrics over time (SCD Type 2).

## Execution Steps
**Step 1:** Run ETL
- **DAG:** open_meteo_weather_etl
- **Action:** Fetches 365 days of London weather history.
- **Feature:** Implements idempotency using TRUNCATE + INSERT within a transaction.
- **Output:** Populates RAW.WEATHER_HISTORY.

**Step 2:** Run ELT (dbt)
- **DAG:** dbt_transformation_dag
- **Action:** Triggers the following dbt commands in order:
- **1.dbt run -** Builds the staging views and analytics tables.
- **2.dbt test -**  Validates schema constraints defined in schema.yml.
- **3.dbt snapshot -** Updates the snapshot table in ANALYTICS.
- **Transformations:**
  - Calculates 7-Day Moving Average for Max Temperature.
  - Categorizes days as "Rainy" or "Dry" based on precipitation.
- **Output:** Creates/Updates ANALYTICS.WEATHER_ANALYTICS and ANALYTICS.WEATHER_SNAPSHOT.


## Visualization

The Power BI dashboard connects to the ANALYTICS.WEATHER_ANALYTICS table.

<img width="1167" height="658" alt="Screenshot 2025-11-20 233154" src="https://github.com/user-attachments/assets/c1279bb1-75d7-456f-b934-47c4444986b9" />

## Key Insights Visualized:
- **Temperature Volatility:** Comparison of daily max temp vs. 7-day moving average.
- **Rainfall Timeline:** Analysis of daily precipitation intensity.
- **Weather Distribution:** Donut chart showing the ratio of rainy vs. dry days in 2023.
- **KPIs:** Cards displaying Maximum Wind Speed and Total Rainfall for the year.
