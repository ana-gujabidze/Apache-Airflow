from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator
from libs.utils import find_time_v_1

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}


dag = DAG(
    dag_id='my_dag3',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
)


with dag:
    get_time = PythonOperator(
        task_id='get_time',
        python_callable=find_time_v_1,
    )
    slack = ConnectionSlackPostOperator(
        task_id='slack',
        conn_id='airflow_2',
        text='{{ task_instance.xcom_pull(task_ids="get_time") }}',
        channel='C020E9BJD26',
    )
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "run_id={{ run_id }}"',
    )
get_time >> slack >> bash_task
