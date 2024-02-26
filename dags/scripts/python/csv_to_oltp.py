import os
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np


def process_users():
    ''' Обрабатывает таблицу пользователей '''

    df = pd.read_csv('/opt/airflow/csv_files/users.csv')

    # Изменяет формат исходной даты для дальнейшего импорта в коннектор
    df['birth_date'] = pd.to_datetime(df['birth_date'], format='%d/%m/%y')
    
    # Добавляет колонку с датой регистрации по умолчанию и меняет тип значений
    df = df.assign(reg_date='2022-08-24')
    df['reg_date'] = pd.to_datetime(df['reg_date'], format="%Y-%m-%d")

    # Добавляет колонку с датой обновления и присвоим значения по умолчанию
    df = df.assign(update_ts='2022-08-24 00:00:00')
    
    # Изменяет формат исходной даты для дальнейшего импорта в коннектор
    df['update_ts'] = pd.to_datetime(df['update_ts'], format="%Y-%m-%d %H:%M:%S")
    
    # Заполняет недостающие порядковые значения в столбце
    new_user_id = pd.DataFrame({'user_id': range(1, df['user_id'].max() + 1)})
    df = pd.merge(new_user_id, df, on='user_id', how='left')

    # Заменяет отсутствующие значения верным типом данных
    df = df.replace({np.nan: None})

    return df


def process_orders():
    ''' Обрабатывает таблицу заказов '''

    df = pd.read_csv('/opt/airflow/csv_files/orders.csv')

    # Изменяет формат исходной даты для дальнейшего импорта в коннектор
    df['creation_time'] = pd.to_datetime(df['creation_time'], format='%d/%m/%y %H:%M')

    # Модифицирует исходный формат массива в формат массива в Постгрес
    df['product_ids'] = df['product_ids'].str.replace('[', '{').str.replace(']', '}').str.replace('"', '')  
    df['product_quant'] = df['product_quant'].str.replace('[', '{').str.replace(']', '}').str.replace('"', '')
    
    new_order_id = pd.DataFrame({'order_id': range(1, df['order_id'].max() + 1)})
    df = pd.merge(new_order_id, df, on='order_id', how='left')
    
    df = df.replace({np.nan: None})

    return df


def process_actions():
    ''' Обрабатывает таблицу действий пользователей '''

    df = pd.read_csv('/opt/airflow/csv_files/user_actions.csv')
    
    df['action_time'] = pd.to_datetime(df['action_time'], format='%d/%m/%y %H:%M')
    
    df = df.replace({np.nan: None})    

    return df


def process_products():
    
    ''' Обрабатывает таблицу пользователей '''

    df = pd.read_csv('/opt/airflow/csv_files/products.csv')

    df = df.assign(update_ts='2022-08-24 00:00:00')
    df['update_ts'] = pd.to_datetime(df['update_ts'], format="%Y-%m-%d %H:%M:%S")
    
    df = df.replace({np.nan: None})    

    return df


def write_to_db(df, table, cur):
    
    ''' 
    Создаёт и исполняет код в коннекторе для переданного
    датафрейма, значения 'схема.таблица', курсора
    '''
    
    df_columns = list(df)

    # Создаёт строку из колонок в формате: col1, col2,...
    columns = ",".join(df_columns)

    # Создаёт строку для ввода значений через .format: '%s', '%s', ...
    values_str = "{}".format(",".join(['%s' for _ in df_columns]))

    # Создаёт строку INSERT INTO table (columns) VALUES('%s',...)
    query = "INSERT INTO {} ({}) VALUES ({});".format(table, columns, values_str)
    
    # Создаёт итератор с кортежами значений для их ввода из ДатаФрейма Пандас
    tuples = (tuple(x) for x in df.values)    

    psycopg2.extras.execute_batch(cur, query, tuples, page_size=1000)

def main():  
    ''' 
    Единожды открывает курсор и соединение для всех таблиц.
    Корректно закрывает курсор и соединение при ошибке.
    https://www.psycopg.org/docs/usage.html
    '''

    conn = psycopg2.connect(
        host=os.getenv("PG_SRC_HOST"),
        port=os.getenv("PG_SRC_PORT"),
        dbname=os.getenv("PG_SRC_DBNAME"),
        user=os.getenv("PG_SRC_USERNAME"),
        password=os.getenv("PG_SRC_PASSWORD")
    )
    conn.autocommit=False

    cur = conn.cursor()

    try:
        # Записываем файлы
        write_to_db(process_users(), 'source.users', cur)
        write_to_db(process_orders(), 'source.orders', cur)
        write_to_db(process_products(), 'source.products', cur)
        write_to_db(process_actions(), 'source.user_actions', cur)
        
        conn.commit()

    except Exception as exc:
        print(exc)
    else:
        print('Inserted records successfully')    
    finally:
        conn.close()
        cur.close()


if __name__ == "__main__":
    main()
