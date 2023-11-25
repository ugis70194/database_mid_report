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

  
def createRandomUserDataFromCSV(filename: str):
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

BRANCH = ["小金井支店", "三鷹支店", "国分寺支店"]
requestIndexes = {
  "小金井支店": dict[str, str](),
  "三鷹支店"  : dict[str, str](), 
  "国分寺支店": dict[str, str]()
}
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
    #print(dateStr, requestIndexes[BRANCH[branchIndex]][dateStr])
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
  
def main():
  cnx      = connectDB()
  cursor   = cnx.cursor()

  rice     = fetchTableData(cursor, 'rice')
  salad    = fetchTableData(cursor, 'salad')
  soup     = fetchTableData(cursor, 'soup')
  mainDish = fetchTableData(cursor, 'main_dish')
  userData = fetchTableData(cursor, 'user_data')

  #createRandomUserDataFromCSV('personal_infomation.csv')
  #insertInitialUserData(cnx, cursor)

  recordCounts = [100 * i for i in range(1, 51)]

  insertPerf = {}
  for recordCount in recordCounts:
    dropRequestTable(cnx, cursor)
    createRequestTable(cnx, cursor)
    requestData = [
      createRandomRequestData(
        rice, salad, soup, mainDish, userData
      ) for _ in range(recordCount)
    ]
    start = time.perf_counter()
    for req in requestData:
      insertRequestData(cursor, req)
    cnx.commit()
    end = time.perf_counter()
    insertPerf[str(recordCount)] = end - start

    with open("./insert_performance.csv", mode="w") as f:
      f.write("records, perf[sec]\n")
      for key, val in insertPerf.items():
        f.write(f"{key}, {val}\n")


  cursor.close()
  cnx.close()

if __name__ == "__main__":
  main()