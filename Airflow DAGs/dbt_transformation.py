from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Defining the path for dbt project inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt" 

conn = BaseHook.get_connection('snowflake_conn')

default_args = {
    "owner": "student",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    # Inject Snowflake credentials into environment variables for dbt profiles.yml
    "env": {
        "DBT_USER": conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_ACCOUNT": conn.extra_dejson.get("account"),
        "DBT_SCHEMA": "ANALYTICS", # Target schema
        "DBT_DATABASE": conn.extra_dejson.get("database"),
        "DBT_ROLE": conn.extra_dejson.get("role"),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
        "DBT_TYPE": "snowflake"
    }
}

with DAG(
    "dbt_transformation_dag",
    default_args=default_args,
    schedule=None,
    description="Run dbt transformations for Weather Data",
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test