import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_recordCount():
    # Database connection configuration
    SERVER = 'localhost'  # Your SQL Server instance
    DATABASE = 'customer_warehouse'  # We'll create this database

    # Connect to our customer warehouse database
    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

   # Set up SQLAlchemy engine for pandas compatibility
    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    # 1. Record count validation
    count_query = "SELECT COUNT(*) FROM customer_enriched"
    total_records = pd.read_sql(count_query, engine).iloc[0, 0]
    print(f"Total records in database: {total_records}")

   