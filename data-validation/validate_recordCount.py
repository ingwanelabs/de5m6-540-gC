import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_recordCount():
    SERVER = 'localhost'
    DATABASE = 'customer_warehouse'


    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'


    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    count_query = "SELECT COUNT(*) FROM customer_enriched"
    total_records = pd.read_sql(count_query, engine).iloc[0, 0]
    print(f"Total records in database: {total_records}")

   