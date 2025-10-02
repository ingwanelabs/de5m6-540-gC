import pyodbc
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection_string():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )


def generate_enrichment_status_report():
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("ENRICHMENT STATUS REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        

        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN geo_enriched = 1 THEN 1 ELSE 0 END) as geo_enriched,
                SUM(CASE WHEN is_business = 1 THEN 1 ELSE 0 END) as business_enriched,
                SUM(CASE WHEN calculated_risk IS NOT NULL THEN 1 ELSE 0 END) as risk_calculated
            FROM customer_enriched
        """)
        
        row = cursor.fetchone()
        total = row.total_records
        
        print("Enrichment Coverage:")
        print(f"  Total Records: {total}")
        print(f"  Geographic Enrichment: {row.geo_enriched}/{total} ({row.geo_enriched/total*100:.1f}%)")
        print(f"  Business Classification: {row.business_enriched}/{total} ({row.business_enriched/total*100:.1f}%)")
        print(f"  Risk Assessment: {row.risk_calculated}/{total} ({row.risk_calculated/total*100:.1f}%)")
        

        cursor.execute("""
            SELECT 
                enrichment_status,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            GROUP BY enrichment_status
            ORDER BY count DESC
        """)
        
        print("\n\nEnrichment Status Breakdown:")
        for row in cursor.fetchall():
            print(f"  {row.enrichment_status}: {row.count} ({row.percentage}%)")
        

        cursor.execute("""
            SELECT 
                data_source,
                COUNT(*) as count,
                MIN(processed_date) as first_processed,
                MAX(processed_date) as last_processed
            FROM customer_enriched
            GROUP BY data_source
            ORDER BY count DESC
        """)
        
        print("\n\nData Source Tracking:")
        for row in cursor.fetchall():
            print(f"\n  Source: {row.data_source}")
            print(f"    Records: {row.count}")
            print(f"    First Processed: {row.first_processed}")
            print(f"    Last Processed: {row.last_processed}")
        

        cursor.execute("""
            SELECT TOP 5
                batch_id,
                records_processed,
                records_successful,
                records_failed,
                processing_start,
                DATEDIFF(SECOND, processing_start, processing_end) as duration_seconds
            FROM enrichment_audit
            ORDER BY processing_start DESC
        """)
        
        print("\n\nRecent Processing Batches:")
        for idx, row in enumerate(cursor.fetchall(), 1):
            success_rate = (row.records_successful / row.records_processed * 100) if row.records_processed > 0 else 0
            print(f"\n  Batch {idx}: {str(row.batch_id)[:8]}...")
            print(f"    Processed: {row.records_processed} records")
            print(f"    Success Rate: {success_rate:.1f}%")
            print(f"    Duration: {row.duration_seconds}s")
            print(f"    Timestamp: {row.processing_start}")
        
        print(f"\n{'='*60}")
        print("Report generation completed successfully")
        print(f"{'='*60}\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error generating enrichment status report: {e}")
        return False


if __name__ == "__main__":
    generate_enrichment_status_report()
