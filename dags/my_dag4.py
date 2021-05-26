from datetime import datetime as dt
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator
from libs.utils import find_time_v_2

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': dt(2021, 4, 30, 4, 0),
    'retries': 0,
}

dag = DAG(
    dag_id='my_dag4',
    default_args=default_args,
    schedule_interval='0 4-14 * * *',
    catchup=False,
)


with dag:
    get_time = PythonOperator(
        task_id='get_time',
        python_callable=find_time_v_2,
    )
    slack = ConnectionSlackPostOperator(
        task_id='slack',
        conn_id='airflow_2',
        text='{{ task_instance.xcom_pull(task_ids="get_time") }}',
        channel='C020E9BJD26',
    )

    dag.doc_md = __doc__

    slack.doc_md = dedent(
        """
    #### Task Documentation
    This task sends message to slack private channel:airflow-exercise
    everyday since 2021/04/30 08:00-18:00 about what time it is.
    """
    )
get_time >> slack
