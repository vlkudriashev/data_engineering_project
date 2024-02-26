from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime, timedelta
import scripts.python.csv_to_oltp as csv_to_oltp


default_args = {
	'depends_on_past': False,
	'catchup': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

with DAG(
		'1_csv_to_source',
		default_args=default_args,
		schedule_interval=None,
		start_date=datetime(2022, 8, 25),	# We use the lowest date available in csv file.
		end_date=datetime(2022, 8, 27)
) as dag:


	populate_tables = PythonOperator(
		task_id='populate_tables',
		python_callable=csv_to_oltp.main
	)
	
	modify_tables = SQLExecuteQueryOperator(
		task_id='modify_tables',
		conn_id='PG_SOURCE',
		sql='./scripts/sql/modify_orders_tables.sql',
	)


	populate_tables >> modify_tables
