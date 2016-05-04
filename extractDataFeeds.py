import urllib2
import csv
import mysql.connector
from mysql.connector import errorcode

def is_float(s):
    result = False
    if s.count(".") == 1:
        if s.replace(".", "").isdigit():
            result = True
    return result

def formatColumn(col):
    if is_float(col) or col.isdigit():
        return col
    else:
        return "'" + col + "'"

TABLES = {}

TABLES['stockquotes'] = (
    "CREATE TABLE IF NOT EXISTS stockquotes ("
        "id MEDIUMINT NOT NULL AUTO_INCREMENT, "
        "symbol CHAR(6) NOT NULL, "
        "change_in_percent VARCHAR(30), "
        "last_trade FLOAT, "
        "52_week_low FLOAT, "
        "52_week_high FLOAT, "
        "200_day_moving_average FLOAT, "
        "company_name VARCHAR(100), "
        "PRIMARY KEY (id)"
    ") ENGINE=InnoDB ")

url = 'http://download.finance.yahoo.com/d/quotes.csv?f=sp2l1jkm4n&s=EVN.AX+PRU.AX+RRL.AX+WOR.AX+ANZ.AX+WBC.AX+CBA.AX+MQG.AX+NAB.AX'
response = urllib2.urlopen(url)

table = csv.reader(response)

insertValues = ""
for row in table:
    values = "("
    for column in row:
        values = values + formatColumn(column) + ","
    values = values[:-1]
    values += "),"    
    values += "\n"    
    insertValues = insertValues + values

insertValues = insertValues[:-2]

insertQuery = "INSERT INTO stockquotes (symbol, change_in_percent, last_trade, 52_week_low, 52_week_high, 200_day_moving_average, company_name) VALUES " + insertValues + ";"
print insertQuery

#createQuery = "CREATE TABLE IF NOT EXISTS stockquotes (" + "id MEDIUMINT NOT NULL AUTO_INCREMENT, " + "symbol CHAR(6) NOT NULL, " + "change_in_percent VARCHAR(30), " + "last_trade  FLOAT, " + "52_week_low  FLOAT, " + "52_week_high  FLOAT, " + "200_day_moving_average  FLOAT, " + "company_name VARCHAR(100), " + "PRIMARY KEY (id)" + ");"

try:    
    cnx = mysql.connector.connect(user='financeAdmin', password='passW0rd1',
                              host='localhost',
                              database='finance')
    #create tables
    cursor = cnx.cursor()
    
    for name, ddl in TABLES.iteritems():
        try:
            print("Creating table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    

 #   cursor.execute(createQuery) 
 #   cnx.commit()
    
#    query = "SELECT ID, SYMBOL FROM STOCKQUOTES"
    
    #insert data
    cursor.execute(insertQuery)
    
    # Make sure data is committed to the database
    cnx.commit()
    
    cursor.close()
#    for (id, symbol) in cursor:
#        print("{}, {}").format(id, symbol) 
    

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()
