from airflow import DAG
from airflow.utils.dates import days_ago

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
}


with DAG(
    'my_dag1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    slack = ConnectionSlackPostOperator(
        task_id='send_message',
        conn_id='airflow_2',
        text='Hello World!',
        channel='C020E9BJD26',
    )
