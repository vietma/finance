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

def is_date(s):
    if s.find("/") != -1: #Found Date
        return True
    return False

def is_percent(s):
    if s.find("%") != -1: #Found percent
        return True
    return False

def formatPercentColumn(s):
    if is_percent(s): 
        s = s.replace("%", "")        
    return s  

def formatColumn(col):
    if is_float(col) or col.isdigit():
        return col
    elif is_date(col):
        return "STR_TO_DATE('" + col + "', '%m/%d/%Y')"        
    elif is_percent(col):        
        return formatPercentColumn(col)
    else:
        return "'" + col + "'"
    
def ddlTables(tables):
    for name, ddl in tables.iteritems():
        try:
            print("Creating/Deleting table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    
TABLES_TO_CREATE = {}

TABLES_TO_CREATE['stockquotes'] = (
    "CREATE TABLE IF NOT EXISTS stockquotes ("
        "id MEDIUMINT NOT NULL AUTO_INCREMENT, "
        "symbol CHAR(6) NOT NULL, "
        "change_in_percent FLOAT, "
        "last_trade FLOAT, "
        "52_week_low FLOAT, "
        "52_week_high FLOAT, "
        "50_day_moving_average FLOAT, "
        "percent_change_from_50_day_moving_average FLOAT, "
        "200_day_moving_average FLOAT, "
        "percent_change_from_200_day_moving_average FLOAT, "
        "company_name VARCHAR(100), "
        "last_trade_date DATE, "
        "last_trade_time VARCHAR(20), "        
        "PRIMARY KEY (id), "
        "KEY `symbol` (`symbol`)"
    ") ENGINE=InnoDB"
    )

TABLES_TO_CREATE['oneyearprice'] = (
    "CREATE TABLE IF NOT EXISTS oneyearprice ("
        "symbol CHAR(6) NOT NULL, "
        "52_week_low FLOAT, "
        "52_week_high FLOAT, "
        "PRIMARY KEY (`symbol`), "        
        "CONSTRAINT `oneyearprice_fk` FOREIGN KEY (`symbol`) REFERENCES `stockquotes` (`symbol`) "
        "ON DELETE CASCADE "
        "ON UPDATE CASCADE "
    ") ENGINE=InnoDB"
    )

TABLES_TO_CREATE['crossover'] = (
    "CREATE TABLE IF NOT EXISTS crossover ("
        "symbol CHAR(6) NOT NULL, "
        "crossover_status VARCHAR(6), "
        "50_day_moving_average FLOAT, "
        "200_day_moving_average FLOAT, "
        "PRIMARY KEY (`symbol`), "        
        "CONSTRAINT `crossover_fk` FOREIGN KEY (`symbol`) REFERENCES `stockquotes` (`symbol`) "
        "ON DELETE CASCADE "
        "ON UPDATE CASCADE "
    ") ENGINE=InnoDB"
    )

TABLES_TO_REMOVE = {}

TABLES_TO_REMOVE['crossover'] = (
    "DROP TABLE IF EXISTS crossover CASCADE"                                
    )

TABLES_TO_REMOVE['oneyearprice'] = (
    "DROP TABLE IF EXISTS oneyearprice CASCADE"                                
    )

TABLES_TO_REMOVE['stockquotes'] = (
    "DROP TABLE IF EXISTS stockquotes CASCADE"                                
    )


quotes = "EVN.AX+PRU.AX+RRL.AX+WOR.AX+ANZ.AX+WBC.AX+CBA.AX+MQG.AX+NAB.AX+BHP.AX+RIO.AX+NCM.AX+BAL.AX+SLR.AX+MPL.AX"

url = 'http://download.finance.yahoo.com/d/quotes.csv?f=sp2l1jkm3m8m4m6nd1t1&s=' + quotes
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

insertQuery = "INSERT INTO stockquotes (symbol, change_in_percent, last_trade, 52_week_low, 52_week_high, 50_day_moving_average, percent_change_from_50_day_moving_average, 200_day_moving_average, percent_change_from_200_day_moving_average, company_name, last_trade_date, last_trade_time) VALUES " + insertValues + ";"
print insertQuery

#createQuery = "CREATE TABLE IF NOT EXISTS stockquotes (" + "id MEDIUMINT NOT NULL AUTO_INCREMENT, " + "symbol CHAR(6) NOT NULL, " + "change_in_percent VARCHAR(30), " + "last_trade  FLOAT, " + "52_week_low  FLOAT, " + "52_week_high  FLOAT, " + "200_day_moving_average  FLOAT, " + "company_name VARCHAR(100), " + "PRIMARY KEY (id)" + ");"

try:    
    cnx = mysql.connector.connect(user='financeAdmin', password='passW0rd1',
                              host='localhost',
                              database='finance')
    
    cursor = cnx.cursor()
    
    #delete tables before creating tables
    ddlTables(TABLES_TO_REMOVE)
    
    #create tables
    ddlTables(TABLES_TO_CREATE)

    
#    query = "SELECT ID, SYMBOL FROM STOCKQUOTES"
    
    #insert data
    cursor.execute(insertQuery)
    
    #insert data into oneyearprice
    oneyearpriceData = "INSERT INTO oneyearprice (symbol, 52_week_low, 52_week_high) SELECT symbol, 52_week_low, 52_week_high FROM stockquotes;"
    cursor.execute(oneyearpriceData)
    
    #insert data into crossover
    crossoverData = "INSERT INTO crossover (symbol, crossover_status, 50_day_moving_average, 200_day_moving_average) SELECT symbol, IF(50_day_moving_average - 200_day_moving_average > 0, 'True', 'False') as crossover_status, 50_day_moving_average, 200_day_moving_average FROM stockquotes;"
    cursor.execute(crossoverData)
    
    #insert data into history prices
    checkDate = "SELECT last_trade_date FROM stockquotes LIMIT 1;"
    cursor.execute(checkDate)
    print cursor.fetchone()
    
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
