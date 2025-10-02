import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_auditTrailVerification():
    SERVER = 'localhost'
    DATABASE = 'customer_warehouse'

    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    
    params = urllib.parse.quote_plus(warehouse_connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    audit_query = """
        SELECT 
            batch_id,
            operation_type,
            records_processed,
            records_successful,
            records_failed,
            DATEDIFF(SECOND, processing_start, processing_end) as duration_seconds,
            processing_start
        FROM enrichment_audit 
        ORDER BY processing_start DESC
        """
        
    audit_df = pd.read_sql(audit_query, engine)
    print(f"Recent Processing Batches:")
    for _, row in audit_df.iterrows():
        success_rate = (row['records_successful'] / row['records_processed'] * 100) if row['records_processed'] > 0 else 0
        print(f"   Batch: {str(row['batch_id'])[:8]}... | {row['records_processed']} records | {success_rate:.1f}% success | {row['duration_seconds']}s")


if __name__ == "__main__":
    validate_auditTrailVerification()
