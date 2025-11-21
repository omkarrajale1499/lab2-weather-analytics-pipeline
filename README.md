# Lab 2: Weather Analytics Pipeline (ETL & ELT)

## Project Overview
This project implements an end-to-end data analytics pipeline that fetches historical weather data for London (2023), stores it in **Snowflake**, transforms it using **dbt** (orchestrated by **Airflow**), and visualizes weather trends using **Power BI**.

The goal is to demonstrate an automated workflow combining ETL (Extract, Load) and ELT (Transform) patterns.

## System Architecture
The pipeline consists of the following stages:
1.  **Ingestion (ETL):** Airflow extracts JSON data from the Open-Meteo API and performs a full-refresh load into Snowflake (`RAW` schema).
2.  **Transformation (ELT):** Airflow triggers dbt to clean, aggregate (7-day moving averages), and categorize weather data, materializing tables in the `ANALYTICS` schema.
3.  **Visualization:** Power BI connects to the analytics tables to display insights.

```mermaid
graph LR
    API[Open-Meteo API] -->|ETL DAG| Snowflake_Raw[(Snowflake RAW)]
    Snowflake_Raw -->|dbt Model| Snowflake_Analytics[(Snowflake ANALYTICS)]
    Airflow((Airflow)) -->|Orchestrates| API
    Airflow -->|Trigger| dbt((dbt Core))
    Snowflake_Analytics -->|Import| PowerBI[Power BI Dashboard]
