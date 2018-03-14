###################################################
#                  PACKAGES                       #
###################################################

from __future__ import print_function
import requests
import json
import time
import mysql.connector
from mysql.connector import errorcode
import os

###################################################
#              ENVIRONMENT VARIABLES              #
###################################################

# Symbols from and to
basesym="BTC"
tosym1="USD"
tosym2="EUR"

# Main table name
tab_name="btc_info"

# Defining base request URL (get BTC/USD-EUR price)
url = "https://min-api.cryptocompare.com/data/price?fsym={}&tsyms={},{}".format(basesym, tosym1, tosym2)
time_sleep = 2 # In seconds

# Getting docker environment variables
host_name, port = os.environ.get("SCRAPING_DB_HOST").split(":")
db_name = os.environ.get("SCRAPING_DB_NAME")
db_user = os.environ.get("SCRAPING_DB_USER")
db_pass = os.environ.get("SCRAPING_DB_PASSWORD")


###################################################
#            FUNCTIONS SECTION                    #
###################################################

def access_db(db_user, db_pass, host_name, db_name):
    """Initializes database environment given: 
       user, password, hostname and database name
    """
    # Connecting to database (default port 3306)
    conn = mysql.connector.connect(user=db_user, password=db_pass, host=host_name, database=db_name)

    # Getting cursor
    cursor = conn.cursor()

    return conn, cursor

def create_table(cursor):
    """Prepares database to store information"""
    # Table information
    table_info= """CREATE TABLE {} (
    info_id int(11) NOT NULL AUTO_INCREMENT,
    price_date date NOT NULL,
    price_usd int(11) NOT NULL,
    price_eur int(11) NOT NULL,
    PRIMARY KEY (info_id)
    ) ENGINE=InnoDB""".format(tab_name)

    # Empty table
    try:
        cursor.execute("DROP TABLE IF EXISTS {}".format(tab_name))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Can not delete {} table".format(tab_name))
        else:
            raise

    # Generating empty table
    try:
        print("Creating table {}:".format(tab_name))
        cursor.execute(table_info)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("{} table already exists".format(tab_name))
        else:
            print(err.msg)
    else:
        print("Table {} has been created".format(tab_name))

def get_data(url):
    """Make a http request to get information"""
	# Getting price
    response = requests.get(url)
    data = response.json()

	# Insert salary information
    info_data = {
      'price_date': time.strftime('%Y-%m-%d %H:%M:%S'),
      'price_usd': data[tosym1],
      'price_eur': data[tosym2],
    }
    return info_data

def insert_info(cursor, info_data):
    """Insert streamed information in database"""
    add_info = ("INSERT INTO btc_info (price_date, price_usd, price_eur) VALUES (DATE_FORMAT(%(price_date)s, '%Y-%m-%d %H:%i:%S'), %(price_usd)s, %(price_eur)s)")
    cursor.execute(add_info, info_data)
    
def streaming(cursor, url):
    """Request data to http service and save it in database"""
    while True:
        # Insert scraped data
        insert_info(cursor, get_data(url))
        
        # IMPORTANT: it confirms data into database
        conn.commit()

        # Sleep n seconds (Take info every n seconds)
        time.sleep(time_sleep)

##############################################################
#                    MAIN PROGRAM                            #
##############################################################

conn, cursor = access_db(db_user, db_pass, host_name, db_name)
create_table(cursor)
streaming(cursor, url)

# Closing connection
cursor.close()
conn.close()
