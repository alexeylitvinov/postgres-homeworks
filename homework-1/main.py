"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
import csv
import psycopg2
from dotenv import load_dotenv, find_dotenv

list_path_file = [
    'north_data/employees_data.csv',
    'north_data/customers_data.csv',
    'north_data/orders_data.csv'
]
table_name = ['employees', 'customers', 'orders']
table_columns = [6, 3, 5]
load_dotenv(find_dotenv())
pg_pass = os.environ.get('PGPASS')


def get_arguments(quantity: int) -> str:
    """
    Принимает на вход число (количество столбцов) возвращает строку '%s' с нужным количеством
    '%s' разделенных запятой
    """
    list_symbols = ['%s' for _ in range(quantity)]
    result = ', '.join(list_symbols)
    return result


def get_cdv_reader(path_file: str) -> tuple:
    """
    Принимает на вход путь к .csv файлу и возвращает кортеж с данными из этого файла
    """
    with open(path_file, 'r') as file:
        data = csv.reader(file)
        next(data)
        result_list = tuple(i for i in data)
    return result_list


def writing_data_in_table(data: tuple, table: str, arguments: str) -> None:
    """
    Принимает на вход кортеж с данными с .csv файла, имя таблицы из БД и строку
    аргументов "%s, %s..." по нужному количеству. Записывает данные в таблицу
    """
    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password=pg_pass
    )
    try:
        with conn:
            with conn.cursor() as cur:
                for i in data:
                    cur.execute(f'INSERT INTO {table} VALUES ({arguments})', i)
    except Exception as ex:
        print(ex)
    finally:
        conn.close()


def main():
    for i in range(3):
        data = get_cdv_reader(list_path_file[i])
        table = table_name[i]
        arguments = get_arguments(table_columns[i])
        writing_data_in_table(data, table, arguments)


if __name__ == '__main__':
    main()
