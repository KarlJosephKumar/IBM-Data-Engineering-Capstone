## Task 1 - Implement the function get_last_rowid()

import ibm_db

# connectction details

dsn_hostname = "**enter your own ibm db2 info **.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "abc12345"        # e.g. "abc12345"
dsn_pwd = "7dBZ3wWt9XN6"      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "50000"                # e.g. "50000" 
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
connIbm = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

def get_last_rowid():
    stmt = "Select rowid from sales_data order by rowid DESC limit 1;"
    result = ibm_db.exec_immediate(connIbm, stmt)
    last_row = ibm_db.fetch_assoc(result)
    last_row_id = last_row['ROWID']
    print(last_row_id)
    return last_row_id

last_row_id = get_last_rowid()

## Task 2 - Implement the function get_latest_records()

import mysql.connector

# connect to database
connection = mysql.connector.connect(user='root', password='',host='127.0.0.1',database='sales')

# create cursor
cursor = connection.cursor()

def get_latest_records():
    latest_records_stmt = "Select * from sales_data where rowid > %s;"
    cursor.execute(latest_records_stmt, (last_row_id,))
    rows = cursor.fetchall()
    # printing the first 5 from mysql table after the last_row_id
    for row in rows[0:5]:
        print(row)
    return rows

latest_rows = get_latest_records()

## Task 3 - Implement the function insert_records()
#insert from mysql to db2

from datetime import datetime

def insert_records():
    insert_query = """Insert into your_db2_db_name.sales_data (rowid, product_id, customer_id, quantity, timestamp) \
                values (?, ?, ?, ?, ?)"""
    for row in latest_rows:
        prep_stmt = ibm_db.prepare(connIbm, insert_query)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row_data_with_timestamp = row + (timestamp,)
        ibm_db.execute(prep_stmt, row_data_with_timestamp)
        ibm_db.commit()

insert_records()