###################################################
#                  PACKAGES                       #
###################################################

import redis
import os
import mysql.connector
from flask import Flask, render_template
from flask_bootstrap import Bootstrap


##################################################
#             ENVIRONMENT VARIABLES              #
##################################################

# Symbols
basesym="BTC"
tosym1="USD"
tosym2="EUR"

# Main table name
tab_name="btc_info"

# Flask configuration
app = Flask(__name__)
bootstrap = Bootstrap(app)

# Launch Redis server
cache = redis.Redis(host='redis', port=6379)

# Getting docker environment variables
host_name, port = os.environ.get("WEB_DB_HOST").split(":")
db_name = os.environ.get("WEB_DB_NAME")
db_user = os.environ.get("WEB_DB_USER")
db_pass = os.environ.get("WEB_DB_PASSWORD")


###################################################
#            FUNCTIONS SECTION                    #
###################################################

def access_db(db_user, db_pass, host_name, db_name):
    """Initializes database environment"""
    # Connecting to database
    conn = mysql.connector.connect(user=db_user, password=db_pass, host=host_name, database=db_name)

    # Getting cursor
    cursor = conn.cursor()
   
    return conn, cursor

def show_info(cursor):
    """Make a query to get all table information"""
    query = ("SELECT info_id, DATE_FORMAT(price_date, '%Y-%m-%d %H:%i:%S'), price_usd, price_eur FROM {}".format(tab_name))
    cursor.execute(query)
 
    # Preparing output data
    rows = cursor.fetchall()
    price_date = [item[1] for item in rows]
    price_usd = [item[2] for item in rows]
    price_eur = [item[3] for item in rows]

    # Preparing chart data in dictionary format
    xAxis = {"categories": price_date}
    series = [{"name": tosym1, "data": price_usd}, {"name": tosym2, "data": price_eur}]

    return xAxis, series

@app.route('/')
@app.route('/index')
def index():
    
    # Getting data
    conn, cursor = access_db(db_user, db_pass, host_name, db_name)
    xAxis, series = show_info(cursor)
    
    # Closing connection
    cursor.close()
    conn.close()

    return render_template('index.html', basesym=basesym, xAxis=xAxis, series=series)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
