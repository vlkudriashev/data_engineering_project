from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

from datetime import datetime, timedelta
import scripts.python.api_to_stage as api_to_stage


default_args = {
	'depends_on_past': False,
	'catchup': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

with DAG(
	'3_api_pipeline',
	default_args=default_args,
	# ~ schedule_interval=timedelta(days=1),
	schedule_interval='@once',
	start_date=datetime(2022, 8, 25),
	end_date=datetime(2022, 8, 27)	
) as dag:

	
	api_to_stage = PythonOperator(
		task_id='api_to_stage',
		python_callable=api_to_stage.main,
		op_kwargs={
			'insertion_ts': '{{ data_interval_start.to_datetime_string() }}'
		},
		provide_context=True
	)


	# Задаём значения для конфигурации дальнейших SQL-запросов
	string_args = {
		'table': 'sellers',
		'ts_column': 'insertion_ts',
		'job_time': '{{ data_interval_start.to_datetime_string() }}'
	}
		
	# Вставляем данные из Стейдж таблиц в Таргет и распаковываем
	# значения массивов в отдельные столбцы
	stage_api_to_target = ClickHouseOperator(
		task_id=f"stage_api_to_target",
		clickhouse_conn_id='CLICK_TARGET',
		sql=(
			'''
			DELETE FROM target_db.sellers
			WHERE insertion_ts = '{job_time}'
			'''.format(**string_args),
			'''
			INSERT INTO target_db.sellers
			SELECT
				seller_info.seller_id AS seller_id,
				seller_info.status AS seller_status,
				seller_info.risk_score AS risk_score,
				insertion_ts
			FROM stage_db.sellers
				ARRAY JOIN seller_info
			WHERE 
				{ts_column} = '{job_time}'
			'''.format(**string_args)
		),
		do_xcom_push=False
	)
	
	api_to_stage >> stage_api_to_target