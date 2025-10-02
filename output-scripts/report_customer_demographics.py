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


def generate_customer_demographics_report():
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("CUSTOMER DEMOGRAPHICS REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        

        cursor.execute("SELECT COUNT(*) FROM customer_enriched")
        total_customers = cursor.fetchone()[0]
        print(f"Total Customers: {total_customers}\n")
        

        cursor.execute("""
            SELECT 
                CASE WHEN is_business = 1 THEN 'Business' ELSE 'Personal' END as customer_type,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            GROUP BY is_business
        """)
        
        print("Customer Type Distribution:")
        for row in cursor.fetchall():
            print(f"  {row.customer_type}: {row.count} ({row.percentage}%)")
        

        cursor.execute("""
            SELECT 
                company_size,
                COUNT(*) as count
            FROM customer_enriched
            WHERE is_business = 1
            GROUP BY company_size
            ORDER BY count DESC
        """)
        
        print("\nBusiness Size Distribution:")
        for row in cursor.fetchall():
            print(f"  {row.company_size}: {row.count}")
        

        cursor.execute("""
            SELECT TOP 10
                industry,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            WHERE industry IS NOT NULL AND industry != 'Personal'
            GROUP BY industry
            ORDER BY count DESC
        """)
        
        print("\nTop Industries:")
        for row in cursor.fetchall():
            print(f"  {row.industry}: {row.count} ({row.percentage}%)")
        
        print(f"\n{'='*60}")
        print("Report generation completed successfully")
        print(f"{'='*60}\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error generating demographics report: {e}")
        return False


if __name__ == "__main__":
    generate_customer_demographics_report()
