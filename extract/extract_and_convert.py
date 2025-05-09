import pandas as pd
import mysql.connector
import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa


load_dotenv()
DB_USERNAME = os.getenv('USER')
DB_PASSWORD1 = os.getenv('PASSWORD1')
DB_PASSWORD2 = os.getenv('PASSWORD2')
DB_HOST = os.getenv('HOST')
DB_PORT = os.getenv('PORT')
DB_NAME = os.getenv('DB_NAME')
MONGO_CONN = os.getenv('MONGO_CONN')

script_dir = Path(__file__).resolve().parent
extract_dir = script_dir / "extracted_data"
extract_dir.mkdir(parents=True, exist_ok=True)


# EXTRACT DATA FROM DIFFERENT SOURCES AND CONVERT TO PARQUET
# --- EXTRACT FROM MYSQL ---
conn_mysql = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USERNAME,
    password=DB_PASSWORD1,
    database=DB_NAME
)

tables_mysql = ['orders', 'order_items', 'products']
for table in tables_mysql:
    df = pd.read_sql(f"SELECT * FROM {table}", conn_mysql)
    table_path = extract_dir / f"{table}.parquet"
    table_arrow = pa.Table.from_pandas(df)
    pq.write_table(table_arrow, table_path)

conn_mysql.close()

# --- EXTRACT FROM POSTGRESQL ---
conn_pg = psycopg2.connect(
    host="localhost",
    user="postgres",
    password=DB_PASSWORD2,
    dbname=DB_NAME
)

tables_pg = ['customers', 'inventory_logs', 'returns', 'shipments']
for table in tables_pg:
    df = pd.read_sql(f"SELECT * FROM {table}", conn_pg)
    table_path = extract_dir / f"{table}.parquet"
    table_arrow = pa.Table.from_pandas(df)
    pq.write_table(table_arrow, table_path)

conn_pg.close()

# --- EXTRACT FROM MONGODB ---
client = MongoClient(MONGO_CONN)
db = client[DB_NAME]

collections = ['ad_clicks', 'reviews', 'web_sessions']

for col in collections:
    data = list(db[col].find())
    
    # Remove _id
    for doc in data:
        doc.pop("_id", None)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Save as Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, extract_dir / f"{col}.parquet")



df = pd.read_csv("campaigns01.csv")
table = pa.Table.from_pandas(df)
pq.write_table(table, extract_dir / f"campaigns.parquet")


df = pd.read_excel("cart_activity01.xlsx")
table = pa.Table.from_pandas(df)
pq.write_table(table, extract_dir / f"cart_activity.parquet")