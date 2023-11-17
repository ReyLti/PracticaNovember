import pyodbc
import time

# Замените параметры подключения на свои
server = input("Сервер ")
database = 'import' # любая база для создания строки подключения
username = input("Пользователь ")
password = str(input("Пароль "))
driver = '{ODBC Driver 17 for SQL Server}'

# Формируем строку подключения
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Подключаемся к базе данных
conn = pyodbc.connect(connection_string,autocommit=True)
cursor = conn.cursor()

# Загружаем содержимое .bacpac файла
bacpac_file_path = input("Введите путь файла")
#cursor.execute(r"BEGIN TRANSACTION")
query = f"RESTORE DATABASE RE FROM DISK = '{bacpac_file_path}' WITH FILE = 1, norecovery, replace"
cursor.execute(query)
time.sleep(5)
query = "RESTORE DATABASE RE WITH RECOVERY"
cursor.execute(query)
cursor.close()
conn.close()

