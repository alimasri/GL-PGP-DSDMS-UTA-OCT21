# Import Airflow Operators
from multiprocessing.sharedctypes import Value
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
# Import libraries to handle SQL and dataframes
import pandas as pd
# Import libraries to keep time
from datetime import datetime
# Import functions from config
from utils import send_msg, append_row_sql
import os
import re

# --------------------------------
# Constants
# --------------------------------

FILEPATH = '/home/airflow/gcs/data/input/'
# Mean and Standard Deviations as per the data
mean = 15.45
sd = 32.18
level_3 = mean + 3 * sd
level_6 = mean - 3 * sd

# --------------------------------
# Function Definitions
# --------------------------------

def read_dataframe(file_path):
    df = pd.read_csv(FILEPATH + file_path)
    if df.shape[0] == 0:
        print("Dataframe is empty!")
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    return df

def validate_file_task(**context):
    file_path = os.listdir(FILEPATH)[0]
    # check if we can read and parse the file
    try:
        df = read_dataframe(file_path)
    except ValueError as e:
        print(e)
        return 'validation_failed'

    # check if dates in the csv file match the date in file name
    matches = re.findall(r'.*tx_data_w_(\d{4}_\d{2}_\d{2}).*\.csv', file_path)
    if len(matches) == 0:
        return 'validation_failed'
    date_extr = matches[0]
    start_date_from_file_name = datetime.strptime(date_extr, "%Y_%m_%d")
    match = pd.to_datetime(df['InvoiceDate'].iloc[0])
    if start_date_from_file_name.date() == match.date():
        context['ti'].xcom_push(key="file_path", value=file_path)
        return 'validation_passed'
    else:
        return 'validation_failed'


def ingest_into_db(**context):
    file_path = context['ti'].xcom_pull(key='file_path')
    df = read_dataframe(file_path)
    # Append the row into the T_Transactions SQL table
    append_row_sql(df, 'T_Transactions')


def flag_anomaly(**context):
    file_path = context['ti'].xcom_pull(key='file_path')
    df = read_dataframe(file_path)

    messages = ""
    for i in range(len(df)):
        revenue = df['Revenue'][i]
        if revenue >= level_3:
            messages += f'Anomaly detected! revenue value {revenue} is too high!\n'
        elif revenue <= level_6:
            messages += f'Anomaly detected! revenue value {revenue} is too low\n'
    if messages:
        send_msg(messages)


# --------------------------------
# DAG definition
# --------------------------------
default_args = {
    'owner': 'ali_masri',
    'start_date': datetime(2022, 3, 17),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Create the DAG object
with DAG(
    'retail',
    default_args=default_args,
    description='DAG',
    schedule_interval=None,  # Replace with appropriate schedule interval after testing
    catchup=False
) as dag:

    file_sensor = FileSensor(
        task_id='file_sensor',
        poke_interval=1,
        filepath=FILEPATH,
        timeout=2,
        dag=dag
    )

    validate_new_file = BranchPythonOperator(
        task_id="validate_file",
        python_callable=validate_file_task,
        dag=dag,
        provide_context=True
    )

    validation_failed = DummyOperator(task_id="validation_failed", dag=dag)

    validation_passed = DummyOperator(task_id="validation_passed", dag=dag)

    validation_failed_slack = PythonOperator(
        task_id="validation_failed_slack",
        python_callable=send_msg,
        op_kwargs={
            'text_string': 'Validation Failed with Validation Error Code: 5'},
        dag=dag
    )

    ingest_into_db_task = PythonOperator(
        task_id="ingest_into_db_task",
        python_callable=ingest_into_db,
        dag=dag,
        provide_context=True
    )

    flag_anomaly_task = PythonOperator(
        task_id="flag_anomaly_task",
        python_callable=flag_anomaly,
        dag=dag,
        provide_context=True
    )

    start_task = DummyOperator(
        task_id='start_task',
        dag=dag
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag
    )

    start_task >> file_sensor >> validate_new_file
    validate_new_file >> [validation_failed, validation_passed]
    validation_failed >> validation_failed_slack
    validation_passed >> ingest_into_db_task >> flag_anomaly_task >> end_task
    validation_failed_slack >> end_task
