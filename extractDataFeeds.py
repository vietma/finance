import urllib2
import csv
import mysql.connector


url = 'http://download.finance.yahoo.com/d/quotes.csv?f=sp2l1jkm4n&s=EVN.AX+PRU.AX+RRL.AX+WOR.AX+ANZ.AX+WBC.AX'
response = urllib2.urlopen(url)

cr = csv.reader(response)

for row in cr:
    print row

try:    
    cnx = mysql.connector.connect(user='financeAdmin', password='passW0rd1',
                              host='localhost',
                              database='finance')

except mysql.connector.Error as err:
    if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()
