from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

from datetime import datetime, timedelta


default_args = {
	'depends_on_past': True,
	'catchup': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

with DAG(
	'2_oltp_pipeline',
	default_args=default_args,
	# ~ schedule_interval=timedelta(days=1),
	schedule_interval='@once',
	start_date=datetime(2022, 8, 25),
	end_date=datetime(2022, 8, 27)
) as dag:
	
	# Определяем таблицы для обработки и название колонки, которая
	# используется для инкрементальной загрузки
	tables = [
		('users', 'update_ts'),
		('orders', 'creation_time'),
		('order_items', 'creation_time'),
		('user_actions', 'action_time'),
		('products', 'update_ts')
	]

	
	for table in tables:

		# Аргументы для форматирования запросов:	
		string_args = {
			# названия таблиц и колонок для загрузки, извлеченные
			# из кортежей
			'table': table[0],		
			'ts_column': table[1],
			# логическое время выполнения ДАГов в формате "YYYY-MM-DD HH:MM:SS"
			'job_time': '{{ data_interval_start.to_datetime_string() }}',
			# нижняя и верняя границы промежутков за которые получаем данные из таблиц			
			'lower_bound_ts': '{{ data_interval_start.subtract(days=1).to_datetime_string() }}',
			'upper_bound_ts': '{{ data_interval_start.to_datetime_string() }}'
		}
	
		insert_stg_all = ClickHouseOperator(
			task_id=f"insert_stg_{table[0]}",
			clickhouse_conn_id='CLICK_CONN',
			sql=(
				'''
				ALTER TABLE stage_db.{table}
				DROP PARTITION tuple(toYYYYMMDD(toDate('{job_time}')))
				'''.format(**string_args)
				,
				'''
				INSERT INTO stage_db.{table}
				SELECT *, '{job_time}' as insertion_ts 
				FROM postgresql(pg_source_creds, table='{table}')
				WHERE {ts_column} >= '{lower_bound_ts}'
				  AND {ts_column} <  '{upper_bound_ts}'
				'''.format(**string_args),				
			),
			do_xcom_push=False
		)
		
		if table[0] in ['users', 'user_actions']:

			insert_tgt_simple = ClickHouseOperator(
				task_id=f"insert_tgt_{table[0]}",
				clickhouse_conn_id='CLICK_CONN',
				sql=(
					'''
					INSERT INTO target_db.{table}
					SELECT *
					FROM stage_db.{table}
					WHERE insertion_ts = '{job_time}'
					'''.format(**string_args),				
				),
				do_xcom_push=False
			)
		
			insert_stg_all >> insert_tgt_simple
		
		if table[0] in ['orders']:
			# Таск для таблицы с заказами. Соединяем данные о заказах в массивы.
			insert_tgt_orders = ClickHouseOperator(
				task_id=f"insert_tgt_all_orders",
				clickhouse_conn_id='CLICK_CONN',
				sql=(
					'''
					INSERT INTO target_db.orders
					SELECT 
						order_id,
						creation_time,
						groupArray(product_id) AS product_ids, 
						groupArray(product_quant) AS product_quant,
						insertion_ts
					FROM stage_db.order_items
					WHERE insertion_ts = '{job_time}'
					GROUP BY order_id, creation_time, insertion_ts;
					'''.format(**string_args),				
				),
				do_xcom_push=False
			)
			
			insert_stg_all >> insert_tgt_orders
		
		if table[0] == 'order_items':
			insert_stg_all >> insert_tgt_orders
	
		if table[0] == 'products':
			
			# Таск организации SCD для таблицы products
			insert_tgt_products = ClickHouseOperator(
				task_id=f"insert_tgt_products",
				clickhouse_conn_id='CLICK_CONN',
				sql=(
					'''
					-- Реализация SCD 2 в Clickhouse
					
					-- Создаём временную таблицу для хранения вновь введенных записей
					CREATE TEMPORARY TABLE inserted_records as 
					(SELECT 
						product_id,
						name,
						price,
						update_ts as record_start,
						'2100-01-01 00:00:00' as record_end,
						insertion_ts
					FROM stage_db.products
					WHERE insertion_ts = '{{ data_interval_start.to_datetime_string() }}'
					);
					''',
					'''
					-- Создаём временную таблицу для нахождения записей с максимальным
					-- timestamp среди уже имеющихся в Target
					-- (у них record_end будет равен '2100-01-01')
					CREATE TEMPORARY TABLE prev_max_records as
					(SELECT 
					  product_id,	  	
					  name,
					  price,
					  record_start,
					  record_end,
					  insertion_ts
					FROM target_db.products
					WHERE 
						record_end = '2100-01-01 00:00:00' and
						product_id in (select product_id
									from inserted_records)
					);
					''',
					'''
					-- Вносим записи для замены предыдущих "максимальных"
					INSERT INTO target_db.products
						WITH new_records as (
						  SELECT
							  prev.product_id,	  	
							  prev.name,
							  prev.price,
							  prev.record_start as record_start,
							  cur.record_start as record_end,
							  prev.insertion_ts
						  FROM
							  prev_max_records as prev join inserted_records as cur
							  using (product_id)
						)
					SELECT *
					FROM new_records;
					''',
					'''
					-- Вводим новые значения с максимальной датой
					INSERT INTO target_db.products
					SELECT
						product_id,
						name,
						price,
						record_start,
						record_end,
						insertion_ts
					FROM inserted_records;
					'''),
				do_xcom_push=False
			)
	
			insert_stg_all >> insert_tgt_products