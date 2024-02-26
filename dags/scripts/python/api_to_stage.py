import os
import requests
from clickhouse_driver import Client
from datetime import datetime


def get_api_data():
    # Определяем константы для работы с АПИ
    api_key = "9d207bf0-10f5-4d8f-a479-22ff5aeff8d1"
    api_url = "http://172.17.0.1:80/sellers"
    api_headers = {
        "x-api-key": api_key
    }

    response = requests.get(
        url=api_url,
        headers=api_headers
    )

    json_records = response.json()

    return json_records


def json_to_db(records, insertion_ts):

    client = Client(
        host=os.getenv('CLICK_STAGE_HOST'),
        port=9000,
        user=os.getenv('CLICK_STAGE_USER'),
        password=os.getenv('CLICK_STAGE_PASSWORD'),
        database=os.getenv('CLICK_STAGE_DB')
        #compression=True
    ) 

    try:

        # Подготавливаем данные для колонок с вложенным типом данных
        values = {}
        values['seller_info'] = records

        # Подготавливаем данные для колонки с временем выполнения задачи
        insertion_ts = datetime.strptime(insertion_ts, "%Y-%m-%d %H:%M:%S")
        values['insertion_ts'] = insertion_ts
        
        # Delete values to follow DELETE-WRITE pattern
        client.execute(
            '''
            ALTER TABLE stage_db.{table}
            DROP PARTITION tuple(toYYYYMMDD(toDate(%(insertion_ts)s)))
        	''', {'insertion_ts': insertion_ts}
        )
        
        # Insert values to DB
        client.execute(
	        '''
        	INSERT INTO sellers VALUES 
        	'''
            ,[values]
        )

    except Exception as exc:
        print('Exited with exception:')
        print(exc)
    else:
        print('Inserted records successfully')
        
def main(**op_kwargs):
    insertion_ts = op_kwargs['insertion_ts']
    json_records = get_api_data()
    json_to_db(json_records, insertion_ts)


if __name__ == "__main__":
    main()
