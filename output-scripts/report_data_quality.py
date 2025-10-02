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


def generate_data_quality_report():
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("DATA QUALITY SUMMARY REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        

        cursor.execute("SELECT COUNT(*) FROM customer_enriched")
        total_records = cursor.fetchone()[0]
        print(f"Total Records: {total_records}\n")
        

        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 1 ELSE 0 END) as has_firstname,
                SUM(CASE WHEN last_name IS NOT NULL AND last_name != '' THEN 1 ELSE 0 END) as has_lastname,
                SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) as has_email,
                SUM(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 ELSE 0 END) as has_phone,
                SUM(CASE WHEN postcode IS NOT NULL AND postcode != '' THEN 1 ELSE 0 END) as has_postcode,
                SUM(CASE WHEN region IS NOT NULL AND region != '' THEN 1 ELSE 0 END) as has_region,
                SUM(CASE WHEN country IS NOT NULL AND country != '' THEN 1 ELSE 0 END) as has_country
            FROM customer_enriched
        """)
        
        row = cursor.fetchone()
        total = row.total
        
        print("Field Completeness:")
        print(f"  First Name: {row.has_firstname}/{total} ({row.has_firstname/total*100:.1f}%)")
        print(f"  Last Name: {row.has_lastname}/{total} ({row.has_lastname/total*100:.1f}%)")
        print(f"  Email: {row.has_email}/{total} ({row.has_email/total*100:.1f}%)")
        print(f"  Phone: {row.has_phone}/{total} ({row.has_phone/total*100:.1f}%)")
        print(f"  Postcode: {row.has_postcode}/{total} ({row.has_postcode/total*100:.1f}%)")
        print(f"  Region: {row.has_region}/{total} ({row.has_region/total*100:.1f}%)")
        print(f"  Country: {row.has_country}/{total} ({row.has_country/total*100:.1f}%)")
        

        quality_score = (
            row.has_firstname + row.has_lastname + row.has_email + 
            row.has_phone + row.has_region + row.has_country
        ) / (total * 6) * 100
        
        print(f"\nOverall Data Quality Score: {quality_score:.1f}%")
        

        cursor.execute("""
            SELECT 
                enrichment_status,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            GROUP BY enrichment_status
        """)
        
        print("\n\nEnrichment Status Distribution:")
        for row in cursor.fetchall():
            print(f"  {row.enrichment_status}: {row.count} ({row.percentage}%)")
        

        cursor.execute("""
            SELECT 
                MIN(processed_date) as oldest,
                MAX(processed_date) as newest,
                COUNT(DISTINCT processed_date) as unique_dates
            FROM customer_enriched
        """)
        
        row = cursor.fetchone()
        print("\n\nData Freshness:")
        print(f"  Oldest record: {row.oldest}")
        print(f"  Newest record: {row.newest}")
        print(f"  Unique processing dates: {row.unique_dates}")
        

        cursor.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM customer_enriched
            GROUP BY status
        """)
        
        print("\n\nAccount Status:")
        for row in cursor.fetchall():
            print(f"  {row.status}: {row.count}")
        
        print(f"\n{'='*60}")
        print("Report generation completed successfully")
        print(f"{'='*60}\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error generating data quality report: {e}")
        return False


if __name__ == "__main__":
    generate_data_quality_report()
