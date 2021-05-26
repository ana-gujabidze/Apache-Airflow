from random import randint

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

from custom_packages.custom_slack_apipost_operator import \
    ConnectionSlackPostOperator

default_args = {
    'owner': 'ana.gujabidze',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}


dag = DAG(
    dag_id='my_dag5',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
)


def finding_rand_num():
    return randint(1, 20)


def choose_best_num(ti):
    nums = ti.xcom_pull(task_ids=['task_1', 'task_2', 'task_3'])
    max_num = max(nums)
    if max_num > 5:
        return 'accurate'
    return 'inaccurate'


with dag:
    tasks = [
        PythonOperator(
            task_id=f"task_{ID}",
            python_callable=finding_rand_num
        ) for ID in range(1, 4)
    ]

    choose_best = BranchPythonOperator(
        task_id='choose_best',
        depends_on_past=True,
        python_callable=choose_best_num,
    )

    accurate = ConnectionSlackPostOperator(
        task_id='accurate',
        conn_id='airflow_2',
        text='accurate',
        channel='C020E9BJD26',
    )

    inaccurate = ConnectionSlackPostOperator(
        task_id='inaccurate',
        conn_id='airflow_2',
        text='inaccurate',
        channel='C020E9BJD26',
    )

tasks >> choose_best >> [accurate, inaccurate]
