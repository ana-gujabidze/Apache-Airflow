from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator
from libs.utils import current_weather

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}


dag = DAG(
    dag_id='my_dag6',
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup=False,
)


with dag:
    get_weather = PythonOperator(
        task_id='get_weather',
        python_callable=current_weather,
        op_args=('{{var.value.weather_api_key}}', )
    )
    send_weather = ConnectionSlackPostOperator(
        task_id='send_weather',
        conn_id='airflow_2',
        text='{{ task_instance.xcom_pull(task_ids="get_weather") }}',
        channel='C020E9BJD26',
    )

    dag.doc_md = __doc__

    send_weather.doc_md = dedent(
        """
    #### Task Documentation
    This task sends message to slack private channel:airflow-exercise
    everyday at 10 am UTC about weather in Tbilisi.
    """
    )
get_weather >> send_weather
