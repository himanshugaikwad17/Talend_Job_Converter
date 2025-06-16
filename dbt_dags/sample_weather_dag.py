"""Sample DBT-style Airflow DAG for weather data."""

import logging
from datetime import datetime
from typing import List
import jinja2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException

# Example placeholder for HANA connection hook
from airflow.hooks.base import BaseHook

DAG_ID = "weather_eod_actuals_quotingengine_lwt_v1"
DAG_DESCRIPTION = (
    "This DAG connects to HANA and determines the max date of weather data, "
    "then fetches the latest EOD weather actuals from Snowflake and pushes them to HANA."
)

DEFAULT_ARGS = {
    "start_date": datetime(2020, 7, 9),
}

HANA_CONN = "HANA_CONN"
SNOWFLAKE_CONNECTION = "SNOWFLAKE_CONN"


# ------------------------------
# Helper functions
# ------------------------------

def get_qe_maxdate() -> str:
    """Extract weather max date from HANA."""
    hana_hook = BaseHook.get_connection(HANA_CONN)
    logging.info("Reading max date from HANA %s", hana_hook.host)
    return "2023-01-01"  # placeholder


def get_rec_cnt() -> str:
    """Return SQL to count station IDs."""
    return "select distinct stn_id from WEATHER.qe_stations_list_vw"


def get_wthr_sfsql() -> str:
    """Return SQL for weather actuals from Snowflake."""
    p_start_date = get_qe_maxdate()
    return f"select * from WEATHER_DATA where actl_date > '{p_start_date}'"


def fetch_whtr_results() -> List:
    """Fetch weather actuals from Snowflake."""
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION)
    results = sf_hook.get_records(get_wthr_sfsql())
    logging.info("Fetched %s records from Snowflake", len(results))
    return results


def write_to_hana() -> None:
    """Write the records to HANA."""
    records = fetch_whtr_results()
    hana_hook = BaseHook.get_connection(HANA_CONN)
    logging.info("Writing %s rows to HANA %s", len(records), hana_hook.host)
    # Placeholder: insert rows using hana_hook


def get_less_count_maxdate() -> str:
    """Example check for incomplete data in HANA."""
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION)
    rec_count = len(sf_hook.get_records(get_rec_cnt()))
    hana_hook = BaseHook.get_connection(HANA_CONN)
    logging.info("Checking for less-count max date in HANA %s", hana_hook.host)
    return "2023-01-01"  # placeholder


def is_second_sunday_of_march(date_str: str) -> bool:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.month == 3 and date_obj.weekday() == 6 and 7 <= date_obj.day <= 14


def delete_less_count_data() -> int:
    """Delete data from HANA if counts are lower than expected."""
    date_str = get_less_count_maxdate()
    if is_second_sunday_of_march(date_str):
        raise AirflowSkipException
    hana_hook = BaseHook.get_connection(HANA_CONN)
    logging.info("Deleting records in HANA after %s", date_str)
    return 0  # placeholder count


# ------------------------------
# DAG definition
# ------------------------------
with DAG(
    catchup=False,
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
    schedule="05 21 * * *",
    template_undefined=jinja2.Undefined,
) as dag:
    put_to_hana = PythonOperator(
        task_id="put_to_hana",
        python_callable=write_to_hana,
        trigger_rule="none_failed",
    )
    delete_less_records_task = PythonOperator(
        task_id="delete_less_records_from_hana",
        python_callable=delete_less_count_data,
    )
    delete_less_records_task >> put_to_hana
