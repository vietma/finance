import urllib2
import csv
import mysql.connector
from mysql.connector import errorcode
from helper import ddlTables
from helper import formatColumn
from helper import getSymbols
from helper import totalNumberOfSymbols



def getStockData(symbols, criteria):
    #sp2l1jkm3m8m4m6nd1t1
    url = 'http://download.finance.yahoo.com/d/quotes.csv?f=' + criteria +'&s=' + symbols
    response = urllib2.urlopen(url)
    
    table = csv.reader(response)
    
    insertValues = ""
    for row in table:        
        values = "("
        col_idx = 0
        for column in row:            
            col_idx += 1
            if (column.strip() == "+inf%") or (column.strip() == 'N/A'):                
                values = ""
                break
            else:
                if col_idx < len(row):
                    lineending = ","
                else:
                    lineending = "),\n"
                values = values + formatColumn(column) + lineending
        if col_idx == len(row):
            insertValues += values        
    
    return insertValues[:-2]
    
    
    
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

# TABLES_TO_REMOVE = {}
# 
# TABLES_TO_REMOVE['crossover'] = (
#     "DROP TABLE IF EXISTS crossover CASCADE"                                
#     )
# 
# TABLES_TO_REMOVE['oneyearprice'] = (
#     "DROP TABLE IF EXISTS oneyearprice CASCADE"                                
#     )
# 
# TABLES_TO_REMOVE['stockquotes'] = (
#     "DROP TABLE IF EXISTS stockquotes CASCADE"                                
#     )

TABLES_TO_REMOVE = ["DROP TABLE IF EXISTS crossover CASCADE", "DROP TABLE IF EXISTS oneyearprice CASCADE", "DROP TABLE IF EXISTS stockquotes CASCADE"]


try:    
    cnx = mysql.connector.connect(user='financeAdmin', password='passW0rd1',
                              host='localhost',
                              database='finance')
    
    cursor = cnx.cursor()
    
    #delete tables before creating tables
#     ddlTables(cursor, TABLES_TO_REMOVE)
    for i in TABLES_TO_REMOVE:
        cursor.execute(i)
    
    
    #create tables
    ddlTables(cursor, TABLES_TO_CREATE)

        
    criteria = "sp2l1jkm3m8m4m6nd1t1"
    
    startPos = 0
    offset = 40
    symbolNumber = totalNumberOfSymbols(cursor)
    
    while startPos < symbolNumber:           
        insertValues = getStockData(getSymbols(cursor, startPos, offset), criteria)
        startPos += offset
        
        insertQuery = ("INSERT INTO stockquotes "
                       "(symbol, change_in_percent, last_trade, 52_week_low, 52_week_high, 50_day_moving_average, percent_change_from_50_day_moving_average, 200_day_moving_average, percent_change_from_200_day_moving_average, company_name, last_trade_date, last_trade_time) "
                       "VALUES " + insertValues)
    
        print insertQuery
        #insert data
        cursor.execute(insertQuery)
    
    #insert data into oneyearprice
    oneyearpriceData = "INSERT INTO oneyearprice (symbol, 52_week_low, 52_week_high) SELECT symbol, 52_week_low, 52_week_high FROM stockquotes;"
    cursor.execute(oneyearpriceData)
     
    #insert data into crossover
    crossoverData = "INSERT INTO crossover (symbol, crossover_status, 50_day_moving_average, 200_day_moving_average) SELECT symbol, IF(50_day_moving_average - 200_day_moving_average > 0, 'True', 'False') as crossover_status, 50_day_moving_average, 200_day_moving_average FROM stockquotes;"
    cursor.execute(crossoverData)
#     
#     #insert data into history prices
#     checkDate = "SELECT last_trade_date FROM stockquotes LIMIT 1;"
#     cursor.execute(checkDate)
#     print cursor.fetchone()
    
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
