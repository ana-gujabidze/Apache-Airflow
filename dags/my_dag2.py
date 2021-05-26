from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
}


dag = DAG(
    dag_id='my_dag2',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
)

with dag:

    slack_1 = ConnectionSlackPostOperator(
        task_id='slack_1',
        conn_id='airflow_2',
        text='Hello',
        channel='C020E9BJD26',
    )

    take_a_break = BashOperator(
        task_id='take_a_break',
        bash_command='sleep 1m',
    )

    slack_2 = ConnectionSlackPostOperator(
        task_id='slack_2',
        conn_id='airflow_2',
        text=' World!',
        channel='C020E9BJD26',
    )

slack_1 >> take_a_break >> slack_2
