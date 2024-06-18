# Задача: написать python скрипт, который будет подключаться к списку БД, таблице accrual_report.csv и удалять дубликаты
# строк. Значения в столбце id_o должны быть уникальными. SQL запрос должен быть написан для MS SQL и PostgreSQL.
#
# Вводные по задаче:
# Для тестирования SQL запроса можно использовать сервис и датафрейм accrual_report.csv.
#
# Результат выполнения:
# .py файл с кодом
# это демоверсия для точной реализации нужно больше времени.
import csv
import psycopg2
import pyodbc

# Подключение к MS SQL
conn_str = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=<your_server_name>;"
    r"DATABASE=<your_database_name>;"
    r"UID=<your_username>;"
    r"PWD=<your_password>;"
)
conn_ms = pyodbc.connect(conn_str)
cursor_ms = conn_ms.cursor()

# Подключение к PostgreSQL
conn_pg = psycopg2.connect(
    host="<your_host>",
    database="<your_database>",
    user="<your_username>",
    password="<your_password>"
)
cursor_pg = conn_pg.cursor()

# Чтение данных из CSV файла
with open('accrual_report.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Пропускаем заголовок
    data = list(reader)

# Удаление дубликатов для MS SQL
delete_query_ms = """
DELETE FROM accrual_report
WHERE id_o IN (
    SELECT id_o
    FROM (
        SELECT id_o, ROW_NUMBER() OVER (PARTITION BY id_o ORDER BY id_o) AS row_num
        FROM accrual_report
    ) AS subquery
    WHERE row_num > 1
)
"""
cursor_ms.execute(delete_query_ms)
conn_ms.commit()

# Удаление дубликатов для PostgreSQL
delete_query_pg = """
DELETE FROM accrual_report
WHERE id_o IN (
    SELECT id_o
    FROM (
        SELECT id_o, ROW_NUMBER() OVER (PARTITION BY id_o ORDER BY id_o) AS row_num
        FROM accrual_report
    ) AS subquery
    WHERE row_num > 1
)
"""
cursor_pg.execute(delete_query_pg)
conn_pg.commit()

# Закрытие соединений
cursor_ms.close()
conn_ms.close()
cursor_pg.close()
conn_pg.close()