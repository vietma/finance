from urllib import urlencode
from urllib2 import Request, urlopen
from collections import OrderedDict
import mysql.connector
from mysql.connector import errorcode
import datetime
# from datetime import date, timedelta

def replace_hyphens(s):
    return s.replace('-', '_')

def round_off_float_to_string(f):
    return "%.2f" % f

def convert_float_to_string(f):
    return "%s" % f

def convert_string_to_float(s):
    return float(s)

def remove_ax(s):
    return s[:-3]

default_historical_data = [] # global variable

def get_dates(start_date, end_date):
#     d1 = convert_string_to_date(start_date)
#     d2 = convert_string_to_date(end_date)
#     delta = d2 - d1
#     
#     dates = []
#     
#     for i in range(delta.days + 1):
#         d = d1 + timedelta(days=i)
#         dates.append(d.strftime("%Y_%m_%d"))
    
    # Choose BHP.AX for default
    global default_historical_data
    if len(default_historical_data) == 0:
        default_historical_data = get_historical_prices('BHP.AX', start_date, end_date)
        
    dates = []
    for day in default_historical_data:
        day_data = day.split(',')
        date_value = replace_hyphens(day_data[0])
        dates.append(date_value)
    
    return dates

def build_select_query(field_dict):
    column_names = ""
    for column in field_dict.iterkeys():
        column_names += column + ', '
    column_names = column_names[:-2]
    sql = "select " + column_names + " from finance.stockquotes where last_trade_date >= (CURDATE() - INTERVAL 1 DAY) and change_in_percent > 0 and last_trade > 0.1 and volume > average_daily_volume and last_trade*volume > 2000000 order by last_trade_date desc, change_in_percent desc;"
    return sql

def build_primary_fields():
    primary_fields_dict = OrderedDict()
    primary_fields_dict['symbol'] = {
        'type': 'CHAR(6) NOT NULL',
        'value': ""
    }    
        
    primary_fields_dict['change_in_percent'] = {
        'type': 'FLOAT',
        'value': 0.0
    }
        
    primary_fields_dict['last_trade'] = {
        'type': 'FLOAT',
        'value': 0.0
    }
    
    return primary_fields_dict

def build_historical_date_fields(start_date, end_date):
    date_sub_fields_suffix = ['percent', 'close']
    dates = get_dates(start_date, end_date) 
    
    historical_date_fields_dict = OrderedDict()

    for each_date in dates[:-1]:
        for each_suffix in date_sub_fields_suffix:
            date_column_name = each_date + "_" + each_suffix
            historical_date_fields_dict[date_column_name] = {
                'type': 'FLOAT',
                'value': 0.0
            }
            
    return historical_date_fields_dict

def build_table_structure(start_date, end_date):
    
    table_structure_dict = build_primary_fields() 
    
    historical_dates_dict = build_historical_date_fields(start_date, end_date)
    
    for each_date in historical_dates_dict.iterkeys():
        table_structure_dict[each_date] = historical_dates_dict[each_date]
            
    return table_structure_dict

# Get Historical Prices for a stock symbol
def get_historical_prices(symbol, start_date, end_date):
    # start_date and end_date are in format 'YYYY-MM-DD'
    params = urlencode({
        's': symbol,
        'a': int(start_date[5:7]) - 1, # MM
        #'b': int(start_date[8:10]) - 1,    # DD - Get one extra previous day to calculate percent change - defect when from date is the 1st
        'b': int(start_date[8:10]), # DD
        'c': int(start_date[0:4]),     # YYYY 
        'd': int(end_date[5:7]) - 1,
        'e': int(end_date[8:10]),
        'f': int(end_date[0:4]),
        'g': 'd',
        'ignore': '.csv'
    })
    endpoint = 'http://real-chart.finance.yahoo.com/table.csv?%s' % params
    print 'historical prices endpoint = %s' % endpoint
        
    request = Request(endpoint)
    response = urlopen(request)
    content = str(response.read().decode('utf-8').strip())
        
    daily_data = content.splitlines()
    return daily_data[1:] 
  

def construct_historical_prices_table(start_date, end_date):
    table_structure_dict = build_table_structure(start_date, end_date)
    all_columns = ""  
    for column_name in table_structure_dict.iterkeys():           
        values = table_structure_dict[column_name]
        column_type = values['type']
        all_columns += column_name + " " + column_type + ", " 
        
    create_table = "CREATE TABLE IF NOT EXISTS historicalprices (" + all_columns[:-2] + ") ENGINE=InnoDB"    
    return create_table

def get_all_historical_prices(bullish_stock_dict, start_date, end_date):    
#     stock_symbols = bullish_stocks.split(',')
    all_column_names = ""
    all_column_values = ""
    index = 0
    for symbol in bullish_stock_dict.iterkeys():
#         stock_historical_prices = get_query_with_historical_prices(symbol, start_date, end_date)
        stock_historical_prices = get_historical_prices(symbol, start_date, end_date)
        
        expected_days = len(get_dates(start_date, end_date)) # Compare with BHP.AX
        
        if len(stock_historical_prices) == expected_days:
            if index == 0:
                # Primary Fields
                primary_fields = build_primary_fields()
                for each_field in primary_fields.iterkeys():                
                    all_column_names += each_field + ","
                
                historical_dates_fields = build_historical_date_fields(start_date, end_date)
                for each_date in historical_dates_fields.iterkeys():
                    all_column_names += each_date + ","
                all_column_names = all_column_names[:-1]
            
            index += 1    
            
            values = bullish_stock_dict[symbol]
            change_in_percent = values['change_in_percent']
            last_trade = values['last_trade']
                
            all_column_values += "(" + "'" + remove_ax(symbol) + "'" + "," + convert_float_to_string(change_in_percent) + "," + convert_float_to_string(last_trade) + ","
            
            close_prices = []    
            for day in stock_historical_prices:
                day_data = day.split(',')
                close_prices.append(convert_string_to_float(day_data[4]))
            
            percent_data = [100 * (a - b) / b for a, b in zip(close_prices[::1], close_prices[1::1])]
            
            i = 0
            history_prices = ""
            for each_close in close_prices[:-1]:                            
                history_prices += round_off_float_to_string(percent_data[i]) + "," + convert_float_to_string(each_close) + ","
                i += 1
             
            all_column_values += history_prices[:-1] + "),"
                
    #             keys[1]: day_data[1] # Open
    #             keys[2]: day_data[2] # High
    #             keys[3]: day_data[3] # Low
    #             keys[4]: day_data[4] # Close
    #             keys[5]: day_data[5] # Volume
    #             keys[6]: day_data[6] # Adj Close 
           
                  
       
    insert_statement = "INSERT INTO historicalprices (" + all_column_names + ") VALUES " + all_column_values[:-1] + ";"
    return insert_statement

def execute():
    try:    
        cnx = mysql.connector.connect(user='financeAdmin', password='passW0rd1',
                                      host='localhost',
                                      database='finance')
    
        cursor = cnx.cursor()
    
        today = datetime.date.today() # YYYY-MM-DD        
        week_ago = today - datetime.timedelta(days=8) # YYYY-MM-DD
        
        start_date = week_ago.strftime('%Y-%m-%d')
        print "start_date: " + start_date
        end_date = today.strftime('%Y-%m-%d')
        print "end_date: " + end_date

        # Drop table historicalprices
        print "DROP TABLE IF EXISTS historicalprices CASCADE"
        cursor.execute("DROP TABLE IF EXISTS historicalprices CASCADE")
    
        # Create table historicalprices        
        table_to_create = construct_historical_prices_table(start_date, end_date)
        print table_to_create
        cursor.execute(table_to_create)
    
        # Insert data into table historicalprices
        # Get a list of bullish stocks
        sql = build_select_query(build_primary_fields())
        print "sql = " + sql
#         sql = "select symbol, change_in_percent, last_trade from finance.stockquotes where last_trade_date >= (CURDATE() - INTERVAL 1 DAY) and change_in_percent > 0 and last_trade > 0.1 and volume > average_daily_volume and last_trade*volume > 2000000 order by last_trade_date desc, change_in_percent desc;"

        cursor.execute(sql)                
        
#         print "Returned number of rows = %d" % len(cursor.fetchall()) 
#         count = cursor.rowcount
#         print "count = %d" % count
                 
#         if count > 0:        
        bullish_stock_dict = OrderedDict()      
        for row in cursor:
            symbol = row[0]
            change_in_percent = row[1]
            last_trade = row[2]
            bullish_stock_dict[symbol] = {
                'change_in_percent': change_in_percent,
                'last_trade': last_trade
            }
          
        insert_query = get_all_historical_prices(bullish_stock_dict, start_date, end_date)
        print "Insert Query = %s" % insert_query 
    
        cursor.execute(insert_query)       
        
        cnx.commit()        
        cursor.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()
    

execute()
