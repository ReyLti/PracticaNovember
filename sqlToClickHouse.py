import clickhouse_connect
import pymssql
import time

serverName = input("Сервер ")
userName = input("Пользователь ")
userPassword = str(input("Пароль "))
conn = pymssql.connect(serverName, userName, userPassword)
cursor = conn.cursor(as_dict=True)
cursor.execute('select name from sys.databases')
print()
y=0
listDB=[]
print("Список бд")
for row in cursor:
    print(str(y)+'. ',end='')
    print(row['name'])
    listDB.append(row['name'])
    y=y+1
print()
conn.close()
y=0
dbName = int(input("Введите номер базы данных "))
conn = pymssql.connect(serverName, userName, userPassword,listDB[dbName])
cursor = conn.cursor(as_dict=True)
cursor.execute('SELECT * FROM sys.objects WHERE type in (N\'U\')')
listTB=[]
for row in cursor:
    print(str(y)+'. ',end='')
    print(row['name'])
    listTB.append(row['name'])
    y=y+1
print()
tbName = int(input("Введите номер таблицы "))
request='CREATE TABLE '+listTB[tbName] + '('
listColumn = []
cursor.execute('SELECT COLUMN_NAME AS [Имя столбца], DATA_TYPE AS [Тип столбца] FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name=\'%s\''%(listTB[tbName]))
for row in cursor:
    listColumn.append(row['Имя столбца'])
    print(row['Имя столбца'], end=' ')
    
    if row['Тип столбца']=='nchar': row['Тип столбца']='String'
    request = request+row['Имя столбца'] +' ' +row['Тип столбца']+','
request = request[:-1]
request = request + ') ENGINE = MergeTree() order by '+listColumn[0]
start = time.time() 
cursor.execute('SELECT * from '+listTB[tbName])
requestData = 'insert into '+ listTB[tbName] +'(*) values '
end = time.time() - start 
print()
for row in cursor:
    string='('
    for i in listColumn:
        string = string+str(row[i])+', '
        print(str(row[i])+' ',end='')
    string=string[:-2]+'),'
    requestData=requestData+string
    print()
print()
print(end)
conn.close()
client = clickhouse_connect.get_client(host='localhost', username='default', password='1245780369')
client.command('create database '+listDB[dbName]+' Engine=Memory')
client.command('use '+listDB[dbName])
client.command(request)
requestData=requestData[:-1]
client.command(requestData)
start = time.time()
client.command('select * from '+listTB[tbName])
print(time.time()-start)

client.command('drop database '+listDB[dbName])

