# DAG: Directed Acyclic Graphs
#  - Used to define and manage the sequences of tasks to execute our logics

import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.data_pipeline import insert_data

dag = DAG(
    dag_id="ecommerce_data",
    default_args={
        "owner": "Ravinthiran",
        "start_date": datetime(2024, 3, 5, 15, 30),
    },
    schedule_interval=None,
    catchup=False
)

write_dat_pg = PythonOperator(
    task_id="write_data_to_pgsql",
    provide_context=True,
    python_callable=insert_data,
    dag=dag
)

write_dat_pg
