import urllib2
import csv
from mysql.connector import Error
from mysql.connector import errorcode
from mysql.connector import connect
from helper import ddlTables
from helper import formatColumn

# def escapeSingleQuote(s):
#     if s.find("'") != -1:                
#         return s.replace("'", "''")
#     return s
# 
# def formatCol(s):
#     return "'" + escapeSingleQuote(s) + "'"

url = "http://www.asx.com.au/asx/research/ASXListedCompanies.csv"
response = urllib2.urlopen(url)
 
table = csv.reader(response)

# all(next(table) for i in range(10))
for i in range(3):
    next(table)


insertValues = "" 
for row in table:
    values = "("
    for column in row:
        values += formatColumn(column) + ","
    values = values[:-1]
    values += "),"    
    values += "\n"
    insertValues += values
    
insertValues = insertValues[:-2]

insertQuery = "INSERT INTO ASXListedCompanies (company_name, symbol, industry_group) VALUES " + insertValues + ";"

# print insertQuery

with open("Output.txt", "w") as text_file:
    text_file.write("{}".format(insertQuery))

TABLES_TO_DELETE = {}

TABLES_TO_DELETE['ASXListedCompanies_delete'] = (
    "DROP TABLE IF EXISTS ASXListedCompanies CASCADE"                                   
    )

TABLES_TO_CREATE = {}

TABLES_TO_CREATE['ASXListedCompanies_create'] = (
    "CREATE TABLE IF NOT EXISTS ASXListedCompanies ("
        "symbol CHAR(6) NOT NULL, "
        "company_name VARCHAR(100), "
        "industry_group VARCHAR(100), "        
        "PRIMARY KEY (`symbol`)"        
    ") ENGINE=InnoDB"                                   
    )


try:
    connection = connect(user='financeAdmin', password='passW0rd1',
                                         host='localhost',
                                         database='finance')
    cursor = connection.cursor()    
    
    ddlTables(cursor, TABLES_TO_DELETE)   
    
    ddlTables(cursor, TABLES_TO_CREATE) 
    
    cursor.execute(insertQuery)
    
    connection.commit()
    cursor.close()
        
except Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    connection.close()


