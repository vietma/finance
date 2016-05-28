from mysql.connector import Error
from mysql.connector import errorcode
from dateutil.parser import parse

        

def ddlTables(cursor, tables):
    for name, ddl in tables.iteritems():
        try:
            print("Creating/Deleting table {}: ".format(name))
            cursor.execute(ddl)
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("table already exists.")
            else:
                print(err.msg)
        else:
            print("Successfully create/delete table {}".format(name))

def getSymbols(cursor, startPos, offset):
    query = ("SELECT symbol FROM ASXListedCompanies "
             "LIMIT %s, %s")
    cursor.execute(query, (startPos, offset))
    allSymbols = ""
    for row in cursor:
        symbol = row[0]        
        allSymbols += "{}".format(symbol) + ".AX" + "+"
    return allSymbols[:-1]    

def totalNumberOfSymbols(cursor):
    query = ("SELECT count(symbol) FROM ASXListedCompanies")
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0]
            
def is_float(s):
    result = False
    if s.count(".") == 1:
        if s.replace(".", "").isdigit():
            result = True
    return result

def is_date(s):
    try:
        if len(s) == 3:
            return False
        elif (s.lower().find("pm") != -1) or (s.lower().find("am") != -1):
            return False
        else:
            parse(s)
            return True
    except ValueError:
        return False
#     if s.find("/") != -1: #Found Date
#         return True
#     return False

def is_percent(s):
    if s.find("%") != -1: #Found percent
        return True
    return False

def formatPercentColumn(s):
    if is_percent(s): 
        s = s.replace("%", "")        
    return s

def escapeSingleQuote(s):
    if s.find("'") != -1:                
        return s.replace("'", "''")
    return s
            
def formatColumn(col):
    if is_float(col) or col.isdigit():
        return col
    elif is_date(col):
        return "STR_TO_DATE('" + col + "', '%m/%d/%Y')"        
    elif is_percent(col):        
        return formatPercentColumn(col)    
    else:
        return "'" + escapeSingleQuote(col) + "'"
#         return "'" + col + "'"
