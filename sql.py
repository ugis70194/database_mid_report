import mysql.connector
from mysql.connector import errorcode
from mysql.connector import cursor
from decimal import *
from random import randrange
import datetime
import time

def connectDB():
  try:
    cnx = mysql.connector.connect(
      user     = 'root',
      password = 'root',
      host     = '127.0.0.1',
      database = 'restaurant'
    )
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)

    return None
  else:
    print('connect sucess')
    return cnx
  
def fetchTableData(cursor: cursor.MySQLCursor, table: str):
  query = "SELECT * FROM {}".format(table)
  cursor.execute(query)

  return cursor.fetchall()

BRANCH = ["小金井支店", "三鷹支店", "国分寺支店"]
requestIndexes = {
  "小金井支店": dict[str, str](),
  "三鷹支店"  : dict[str, str](), 
  "国分寺支店": dict[str, str]()
}
def setUpRequestIndexes(cursor: cursor.MySQLCursor):
  request = fetchTableData(cursor, 'request')
  for req in request:
    day = req[1].replace(hour=0, minute=0, second=0).isoformat()
    requestIndexes[req[0]][day] = 0
  
  for req in request:
    day = req[1].replace(hour=0, minute=0, second=0).isoformat()
    requestIndexes[req[0]][day] = max(int(req[2]), requestIndexes[req[0]][day])

def createRandomRequestData(riceData, soupData, saladData, mainDishData, userData):
  branchIndex        = randrange(0, len(BRANCH))
  userDataIndex      = randrange(0, len(userData))
  riceIndex          = randrange(0, len(riceData))
  soupIndex          = randrange(0, len(soupData))
  saladIndex         = randrange(0, len(saladData))
  mainDishIndex      = randrange(0, len(mainDishData))
  totalPrice:Decimal = (riceData[riceIndex][1] +
                        saladData[saladIndex][1] +
                        soupData[soupIndex][1] + 
                        mainDishData[mainDishIndex][1]) 

  dateTime = datetime.datetime(
    year   = 2023, 
    month  = randrange(1,13),
    day    = randrange(1,29)
  )

  dateStr = dateTime.isoformat()
  if not dateStr in requestIndexes[BRANCH[branchIndex]]:
    requestIndexes[BRANCH[branchIndex]][dateStr] = 1
  else:
    requestIndexes[BRANCH[branchIndex]][dateStr] = requestIndexes[BRANCH[branchIndex]][dateStr] + 1

  dateTime = dateTime.replace(
    hour   = randrange(0,24),
    minute = randrange(0,60),
    second = randrange(0, 60)).isoformat() 

  value = {
    'branch'          : BRANCH[branchIndex],
    'request_datetime': dateTime,
    'request_index'   : requestIndexes[BRANCH[branchIndex]][dateStr],
    'request_user_id' : userData[userDataIndex][0],
    'rice'            : riceData[riceIndex][0],
    'soup'            : saladData[saladIndex][0],
    'salad'           : soupData[soupIndex][0],
    'main_dish'       : mainDishData[mainDishIndex][0],
    'total_price'     : totalPrice,
  }

  return value

def writeRequestData(tableName, tableData):
  with open(f"./table_data/{tableName}.csv", mode="w", encoding='utf-8') as f:
    f.write("branch, request_datetime, request_index, request_user_id, rice, soup, salad, main_dish, total_price\n")
    for tpl in tableData:
      f.write(f"{tpl[0]}, {tpl[1]}, {tpl[2]}, {tpl[3]}, {tpl[4]}, {tpl[5]}, {tpl[6]}, {tpl[7]}, {tpl[8]}\n")

def insertRequestData(
    cursor: cursor.MySQLCursor,
    value : dict[str, str]
):
  query = ("INSERT INTO request "
           "(branch, "
           "request_datetime, request_index, request_user_id, "
           "rice, soup, salad, main_dish, " 
           "total_price) "
           "VALUES (%(branch)s, " 
           "%(request_datetime)s, %(request_index)s, %(request_user_id)s, "
           "%(rice)s, %(soup)s, %(salad)s, %(main_dish)s, "
           "%(total_price)s)")

  cursor.execute(query, value)

def deleteRequestData(
  cursor: cursor.MySQLCursor,
  branch: str,
  request_datetime: str,
  request_index: str
):
  query = ("DELETE FROM request "
             "WHERE branch = '{}' AND request_datetime = '{}' AND request_index = '{}' "
             .format(branch, request_datetime.replace('T', ' '), request_index))
  cursor.execute(query)

def insertRandomRequestData(
  cnx: mysql.connector.MySQLConnection, 
  cursor: cursor.MySQLCursor,
  count: int
):
  rice     = fetchTableData(cursor, 'rice')
  salad    = fetchTableData(cursor, 'salad')
  soup     = fetchTableData(cursor, 'soup')
  mainDish = fetchTableData(cursor, 'main_dish')
  userData = fetchTableData(cursor, 'user_data')

  requestData = [
    createRandomRequestData(
        rice, salad, soup, mainDish, userData
      ) for _ in range(count)
    ]
  
  writeRequestData('insert_request_data', [list(req.values()) for req in requestData])

  with open('insert_request.txt', mode="w", encoding='utf-8') as f:
    f.write(f"実験開始時刻: {datetime.datetime.now().isoformat()}\n")
    start = time.perf_counter_ns()
    for req in requestData:
      insertRequestData(cursor, req)
    cnx.commit()
    end = time.perf_counter_ns()
    f.write(f"計測実行時間: {(end-start) / 1000000} [ms]")

  selected = []
  for req in requestData:
    query = ("SELECT * FROM request "
             "WHERE branch='{}' AND request_datetime = '{}' AND request_index='{}' "
             .format(req['branch'], req['request_datetime'], req['request_index']))
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
      selected.append(result[0])

  writeRequestData('insert_request_result', selected)

  with open('delete_request.txt', mode="w", encoding='utf-8') as f:
    f.write(f"実験開始時刻: {datetime.datetime.now().isoformat()}\n")
    start = time.perf_counter_ns()
    for req in requestData:
      deleteRequestData(cursor, req['branch'], req['request_datetime'], req['request_index'])
    cnx.commit()
    end = time.perf_counter_ns()
    f.write(f"計測実行時間: {(end-start) / 1000000} [ms]")

  selected = []
  for req in requestData:
    query = ("SELECT * FROM request "
             "WHERE branch='{}' AND request_datetime = '{}' AND request_index='{}' "
             .format(req['branch'], req['request_datetime'], req['request_index']))
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) > 0:
      selected.append(result[0])

  writeRequestData('delete_request_result', selected)

def updateUserAddress(
  cnx: mysql.connector.MySQLConnection, 
  cursor: cursor.MySQLCursor,
  user_id: str,
  postal_number: str,
  ship_adress: str
):
  query = ("UPDATE user_data "
          "SET postal_number='{}', ship_address='{}' " 
          "WHERE user_id='{}'".format(postal_number, ship_adress, user_id))
  
  select_query = ("SELECT * FROM user_data WHERE user_id='{}'".format(user_id))

  with open('update_request.txt', mode="w", encoding='utf-8') as f:
    f.write(f"実験開始時刻: {datetime.datetime.now().isoformat()}\n")
    start = time.perf_counter_ns()
    cursor.execute(query)
    cnx.commit()
    end = time.perf_counter_ns()
    f.write(f"計測実行時間: {(end-start) / 1000000} [ms]")

    cursor.execute(select_query)
    result = cursor.fetchall()[0]
    f.write(", ".join(result))

def createShippingData(
  cursor: cursor.MySQLCursor,
  user_id: str,
  branch: str,
  request_datetime: str,
  request_index: str
):
  query = ("SELECT user_name, tel, postal_number, ship_address, rice, soup, salad, main_dish, total_price " 
                "FROM user_data INNER JOIN request ON user_data.user_id = request.request_user_id "
                "WHERE user_id='{}' AND branch='{}' AND request_datetime = '{}' AND request_index='{}'"
                .format(user_id, branch, request_datetime, request_index))

  with open('select_request.txt', mode="w", encoding='utf-8') as f:
    f.write(f"実験開始時刻: {datetime.datetime.now().isoformat()}\n")
    start = time.perf_counter_ns()
    cursor.execute(query)
    end = time.perf_counter_ns()
    f.write(f"計測実行時間: {(end-start) / 1000000} [ms]\n")

    tpl = cursor.fetchall()[0]
    f.write(f"{tpl[0]}, {tpl[1]}, {tpl[2]}, {tpl[3]}, {tpl[4]}, {tpl[5]}, {tpl[6]}, {tpl[7]}, {tpl[8]}\n")


def main():
  cnx      = connectDB()
  cursor   = cnx.cursor()

  rice     = fetchTableData(cursor, 'rice')
  salad    = fetchTableData(cursor, 'salad')
  soup     = fetchTableData(cursor, 'soup')
  mainDish = fetchTableData(cursor, 'main_dish')
  userData = fetchTableData(cursor, 'user_data')
  req      = fetchTableData(cursor, 'request')

  createShippingData(cursor, "M3493785522067783", "三鷹支店", "2023-01-02 08:33:27", "1")

  #setUpRequestIndexes(cursor)
  #insertRandomRequestData(cnx, cursor, 10)

  #with open('table_data/insert_request_data.csv', mode='r', encoding='utf-8') as f:
  #  for line in f:
  #    row = line.split(', ')
  #    if row[0] == 'branch':
  #      pass
  #    else:
  #      deleteRequestData(cnx, cursor, row[0], row[1], row[2])

  original = ["M0514976918844333", "193-0841", "東京都八王子市裏高尾町3-11-20コンフォート裏高尾町108"]
  updated  = ["M0514976918844333", "106-0045", "東京都港区麻布十番3-11-8"]
  #updateUserAddress(cnx, cursor, updated[0], updated[1], updated[2])
  #updateUserAddress(cnx, cursor, original[0], original[1], original[2])
  

  cursor.close()
  cnx.close()

if __name__ == "__main__":
  main()

def writeItemData(tableName, tableData):
  with open(f"./table_data/{tableName}.csv", mode="w", encoding='utf-8') as f:
    f.write("item_name, price\n")
    for tpl in tableData:
      f.write(f"{tpl[0]}, {tpl[1]}\n")

def writeUserData(tableName, tableData):
  with open(f"./table_data/{tableName}.csv", mode="w", encoding='utf-8') as f:
    f.write("user_id, user_name, tel, postal_number, ship_address, card_number\n")
    for tpl in tableData:
      f.write(f"{tpl[0]}, {tpl[1]}, {tpl[2]}, {tpl[3]}, {tpl[4]}, {tpl[5]}\n")

def writeDBcontents(cursor: cursor.MySQLCursor):
  rice     = fetchTableData(cursor, 'rice')
  salad    = fetchTableData(cursor, 'salad')
  soup     = fetchTableData(cursor, 'soup')
  mainDish = fetchTableData(cursor, 'main_dish')
  userData = fetchTableData(cursor, 'user_data')
  req      = fetchTableData(cursor, 'request')

  writeItemData('rice', rice)
  writeItemData('salad', salad)
  writeItemData('soup', soup)
  writeItemData('main_dish', mainDish)
  writeUserData('user_data', userData)
  writeRequestData('request', req)

def createRandomUserDataFromCSV(filename: str):
  # csvファイルから疑似個人情報データ生成サービスを用いて生成した
  # 氏名, 電話番号, 郵便番号, 住所 を読み込む
  userdata = []
  with open(f"./dummy_data/{filename}", mode="r", encoding='utf-8') as f:
    for line in f:
      userID     = 'M' + str(randrange(0, 10000000000000000)).zfill(16)
      cardNumber = str(randrange(0, 10000000000000000)).zfill(16)
      data       = f"{userID},{line[:len(line)-1]},{cardNumber}"
      userdata.append(data)
  
  with open("./dummy_data/user_data.csv", mode="w", encoding='utf-8') as f:
    for data in userdata:
      f.write(data + '\n')

def insertInitialUserData(cnx: mysql.connector.MySQLConnection, cursor: cursor.MySQLCursor):
  query = ("INSERT INTO user_data "
           "(user_id, user_name, tel, postal_number, ship_address, card_number) "
           "VALUES (%s, %s, %s, %s, %s, %s)")
  
  with open("./dummy_data/user_data.csv", mode="r", encoding='utf-8') as f:
    for line in f:
      data    = line.split(',')
      data[5] = data[5].replace('\n', '')
      cursor.execute(query, data)
  
  cnx.commit()

def dropRequestTable(
  cnx: mysql.connector.MySQLConnection, cursor: cursor.MySQLCursor
):
  query = "DROP TABLE request"
  cursor.execute(query)
  cnx.commit()

def createRequestTable(
  cnx: mysql.connector.MySQLConnection, cursor: cursor.MySQLCursor
):
  query = ("CREATE TABLE request("
  "branch VARCHAR(256) NOT NULL,"
  "request_datetime DATETIME NOT NULL,"
  "request_index MEDIUMINT UNSIGNED NOT NULL," 
  "request_user_id VARCHAR(17) NOT NULL,"
  "rice VARCHAR(256) NOT NULL,"
  "soup VARCHAR(256) NOT NULL,"
  "salad VARCHAR(256) NOT NULL,"
  "main_dish VARCHAR(256) NOT NULL,"
  "total_price DECIMAL(16, 2) NOT NULL,"
  "PRIMARY KEY(branch, request_datetime, request_index))"
  )

  cursor.execute(query)
  cnx.commit()