
import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_completeness():
    SERVER = 'localhost'
    DATABASE = 'customer_warehouse'

    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    completeness_query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 1 ELSE 0 END) as complete_names,
            SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as complete_emails,
            SUM(CASE WHEN geo_enriched = 1 THEN 1 ELSE 0 END) as geo_enriched_count,
            SUM(CASE WHEN is_business = 1 THEN 1 ELSE 0 END) as business_customers
        FROM customer_enriched
        """
        
    completeness_df = pd.read_sql(completeness_query, engine)
    comp = completeness_df.iloc[0]
        
    print(f"Name completeness: {comp['complete_names']}/{comp['total_records']} ({comp['complete_names']/comp['total_records']:.1%})")
    print(f"Email completeness: {comp['complete_emails']}/{comp['total_records']} ({comp['complete_emails']/comp['total_records']:.1%})")
    print(f"Geographic enrichment: {comp['geo_enriched_count']}/{comp['total_records']} ({comp['geo_enriched_count']/comp['total_records']:.1%})")
    print(f"Business customers: {comp['business_customers']}/{comp['total_records']} ({comp['business_customers']/comp['total_records']:.1%})")

