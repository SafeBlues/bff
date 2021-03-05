import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv('.env') 

# Connect to server
cnx = mysql.connector.connect(
    host=os.environ.get('HOST'),
    port=os.environ.get('PORT'),
    user=os.environ.get('USER'),
    password=os.environ.get('PASSWORD'))

# Get a cursor
cur = cnx.cursor()

# Execute a query
# cur.execute("SELECT CURDATE()")
cur.execute("USE accounts")
cur.execute("SELECT * FROM admin_accounts")


# Fetch one result
row = cur.fetchone()
print(row)
# print("Current date is: {0}".format(row[0]))

# Close connection
cnx.close()