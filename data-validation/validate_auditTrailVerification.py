import pyodbc
import pandas as pd
import urllib 
from sqlalchemy import create_engine

def validate_auditTrailVerification():
    # Database connection configuration
    SERVER = 'localhost'  # Your SQL Server instance
    DATABASE = 'customer_warehouse'  # We'll create this database

    # Connect to our customer warehouse database
    warehouse_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

    audit_query = """
        SELECT 
            batch_id,
            operation_type,
            records_processed,
            records_successful,
            records_failed,
            duration_seconds,
            processing_start
        FROM enrichment_audit 
        ORDER BY processing_start DESC
        """
        
    audit_df = pd.read_sql(audit_query, engine)
    print(f"Recent Processing Batches:")
    for _, row in audit_df.iterrows():
        success_rate = (row['records_successful'] / row['records_processed'] * 100) if row['records_processed'] > 0 else 0
        print(f"   Batch: {str(row['batch_id'])[:8]}... | {row['records_processed']} records | {success_rate:.1f}% success | {row['duration_seconds']}s")