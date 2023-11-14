import airflow.utils.dates
from airflow import DAG

from airflow.sensors.bash import BashSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from filecmp import cmp
import os

SOURCE_FILE_PATH = '/opt/airflow/dags/files'

dag_example = DAG(
    dag_id='data_pipeline_example',
    start_date=airflow.utils.dates.days_ago(1),
    tags=["examples, data_pipeline"]
)

wait_for_file = BashSensor(
    task_id='wait_for_input_file',
    bash_command="test -f /opt/airflow/dags/files/nba_elo.csv -a -n \"$(find /opt/airflow/dags/files/nba_elo.csv -mtime -1)\"",
    retries=10,
    retry_delay=timedelta(seconds=30),
    timeout=600,  # Maximum amount of time the sensor can run (in seconds)
    mode="poke",  # Use "poke" mode for repeated checks
    poke_interval=60,  # Wait for 60 seconds between checks
    dag=dag_example,
)


def file_validation():
    """
    A placeholder. A file validation typically comprises checking that there are no changes - intentional or mistakes -
    in the file format, i.e. the column sequence, the number, and the format is the same,
    data categories haven't changed, etc.

    :return: True or False
    """
    ...


def check_if_same_as_old_file():
    new_file = os.path.join(SOURCE_FILE_PATH, 'nba_elo.csv')
    old_file = os.path.join(SOURCE_FILE_PATH, 'backup', 'nba_elo_old.csv')
    if os.path.exists(old_file) and not cmp(new_file, old_file):
        return "alternative_route"
    else:
        return "load_stage"


validate_file = PythonOperator(
    task_id="validate_file",
    python_callable=file_validation,
    dag=dag_example,
)

decision_step = BranchPythonOperator(
    task_id="flow_selection",
    python_callable=check_if_same_as_old_file,
)


SOURCE_FILE = os.path.join(SOURCE_FILE_PATH, 'nba_elo.csv')
SF_STAGE = "NBA_ELO"
SQL_PUT_FILE = f"PUT file:///{SOURCE_FILE} @%{SF_STAGE};"

load_stage = SnowflakeOperator(
    task_id="load_stage",
    snowflake_conn_id="sf1",
    sql=SQL_PUT_FILE,
    database="PIPELINE_EXAMPLE",
    schema="PUBLIC",
    dag=dag_example,
)


copy_into_table = CopyFromExternalStageToSnowflakeOperator(
    task_id="copy_into_table",
    snowflake_conn_id="sf1",
    database="PIPELINE_EXAMPLE",
    schema="PUBLIC",
    table='NBA_ELO',
    stage='%NBA_ELO',
    file_format="(type = 'CSV',field_delimiter = ',', skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
)


def alternative():
    """
    The alternative path in which the old file is identical to the new source file so there is no need for processing.
    One of the action in this step may be to send an e-mail to the administrator
    :return: None
    """
    ...


alternative_route = PythonOperator(
    task_id="alternative_route",
    python_callable=alternative,
    dag=dag_example,
)

wait_for_file >> validate_file >> decision_step >> [load_stage, alternative_route]
load_stage >> copy_into_table
