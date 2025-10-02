import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_riskDistribution():
    # Database connection configuration
    SERVER = 'localhost'
    DATABASE = 'customer_warehouse'

    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    
    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    risk_query = """
        SELECT 
            calculated_risk,
            COUNT(*) as customer_count,
            CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
        FROM customer_enriched 
        GROUP BY calculated_risk
        ORDER BY customer_count DESC
        """
        
    risk_df = pd.read_sql(risk_query, engine)
    print(f"Risk Distribution:")
    for _, row in risk_df.iterrows():
        print(f"   {row['calculated_risk']} Risk: {row['customer_count']} customers ({row['percentage']}%)")


if __name__ == "__main__":
    validate_riskDistribution()
