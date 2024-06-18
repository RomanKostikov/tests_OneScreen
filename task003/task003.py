# Задача: написать python скрипт, который будет подключаться к списку БД, таблице rating и заменять данные в столбце sku.
# SQL запрос должен быть написан для MS SQL и PostgreSQL.
# Вводные по задаче:
# нужно импортировать файл df.csv и заменить в таблице БД rating.csv в столбце sku, значения sku_old,
# на sku_new. Для тестирования SQL запроса можно использовать сервис и датафрейм rating.csv.
# Результат выполнения:
# .py файл с кодом
# это демоверсия для точной реализации нужно больше времени.
import pandas as pd
import psycopg2
import pyodbc


# Подключение к MS SQL
def connect_to_mssql():
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;Trusted_Connection=yes;')
    return connection


# Подключение к PostgreSQL
def connect_to_postgresql():
    connection = psycopg2.connect(user="your_user",
                                  password="your_password",
                                  host="your_host",
                                  port="your_port",
                                  database="your_database")
    return connection


# Загрузка CSV-файла в DataFrame
df = pd.read_csv('df.csv')
rating_df = pd.read_csv('rating.csv')


# Замена значений в базе данных MS SQL
def replace_values_mssql(connection):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        old_sku = row['sku_old']
        new_sku = row['sku_new']
        cursor.execute(f"UPDATE rating SET sku = '{new_sku}' WHERE sku = '{old_sku}'")
        connection.commit()
    cursor.close()


# Замена значений в базе данных PostgreSQL
def replace_values_postgresql(connection):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        old_sku = row['sku_old']
        new_sku = row['sku_new']
        cursor.execute(f"UPDATE rating SET sku = '{new_sku}' WHERE sku = '{old_sku}'")
        connection.commit()
    cursor.close()


# Подключение и замена значений в MS SQL
mssql_connection = connect_to_mssql()
replace_values_mssql(mssql_connection)
mssql_connection.close()

# Подключение и замена значений в PostgreSQL
postgresql_connection = connect_to_postgresql()
replace_values_postgresql(postgresql_connection)
postgresql_connection.close()
