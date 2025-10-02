import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_geoDistribution():
    # Database connection configuration
    SERVER = 'localhost'
    DATABASE = 'customer_warehouse'

    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    
    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    geo_query = """
        SELECT TOP 5
            region,
            COUNT(*) as customer_count
        FROM customer_enriched 
        WHERE region IS NOT NULL AND region != 'Unknown'
        GROUP BY region
        ORDER BY customer_count DESC
    """
        
    geo_df = pd.read_sql(geo_query, engine)
    print(f"Top Regions by Customer Count:")
    for _, row in geo_df.iterrows():
        print(f"   {row['region']}: {row['customer_count']} customers")


if __name__ == "__main__":
    validate_geoDistribution()
