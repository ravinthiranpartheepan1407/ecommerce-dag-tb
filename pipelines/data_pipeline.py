# Pipelines:
# - Used to ensure the data flow from one end to another end by undergoing necessary transformations and Integrations.

from datetime import date
import pandas as pd
import psycopg2
import traceback
import logging
import urllib.request
import os
from dotenv import load_dotenv


# def check_na():
read_data = pd.read_csv("./data/archive/data.csv", encoding="ISO-8859-1")
check_na_val = read_data.isna().sum()
# Fix NA
read_data["CustomerID"].fillna(0).astype(int)
print(check_na_val)

# Connecting to postgresql
load_dotenv()
pg_host = os.getenv("host")
pg_db = os.getenv("database")
pg_user = os.getenv("user")
pg_pwd = os.getenv("password")
pg_port = os.getenv("port")

try:
    conn = psycopg2.connect(
        host=pg_host,
        database=pg_db,
        user=pg_user,
        password=pg_pwd,
        port=pg_port
    )
    conn.autocommit = True
    cur = conn.cursor()
    # cmd = ''' CREATE database ecommerce '''
    # cur.execute(cmd)
    # print("Database Created Successfully")
    print("PGSQL Connection Established")
    # conn.close()
except Exception as e:
    print(f"Cannot establish connection: {e}")


# Create Table
def create_table():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS ecommerce_analytics (StockCode VARCHAR(50), Description VARCHAR(
        200), Quantity INTEGER, InvoiceDate DATE, UnitPrice FLOAT, CustomerID VARCHAR(50), Country VARCHAR(50)) """)
        print("Table Created Successfully")
    except Exception as err:
        print(f"Cannot create table successfully: {err}")


# Insert Data from DF to Table
def insert_data():
    insert_elements = 0
    for _, rows in read_data.iterrows():
        query = f"""SELECT COUNT(*) FROM ecommerce_analytics WHERE StockCode = '{rows["StockCode"]}'"""
        # conn.autocommit = True
        cur.execute(query)
        res = cur.fetchone()
        if res[0] == 0:
            insert_elements += 1
            cur.execute("""INSERT INTO ecommerce_analytics(StockCode,Description,Quantity,InvoiceDate,UnitPrice,
            CustomerID,Country) VALUES(%s,%s,%s,%s,%s,%s,%s)""", (str(rows[1]), str(rows[2]), int(rows[3]),
                                                                  str(rows[4]), float(rows[5]), str(rows[6]),
                                                                  str(rows[7])))
        print(f"Data Inserted Successfully: {_}")
        # conn.close()


# check_na()
