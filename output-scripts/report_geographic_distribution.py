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
    )


def generate_geographic_distribution_report():
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("GEOGRAPHIC DISTRIBUTION REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        

        cursor.execute("""
            SELECT 
                country,
                COUNT(*) as customer_count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            WHERE country IS NOT NULL AND country != ''
            GROUP BY country
            ORDER BY customer_count DESC
        """)
        
        print("Distribution by Country:")
        for row in cursor.fetchall():
            print(f"  {row.country}: {row.customer_count} ({row.percentage}%)")
        

        cursor.execute("""
            SELECT 
                region,
                COUNT(*) as customer_count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            WHERE region IS NOT NULL AND region != 'Unknown' AND region != ''
            GROUP BY region
            ORDER BY customer_count DESC
        """)
        
        print("\n\nDistribution by Region:")
        for row in cursor.fetchall():
            print(f"  {row.region}: {row.customer_count} ({row.percentage}%)")
        

        cursor.execute("""
            SELECT TOP 15
                district,
                COUNT(*) as customer_count
            FROM customer_enriched
            WHERE district IS NOT NULL AND district != ''
            GROUP BY district
            ORDER BY customer_count DESC
        """)
        
        print("\n\nTop Districts/Cities:")
        for idx, row in enumerate(cursor.fetchall(), 1):
            print(f"  {idx}. {row.district}: {row.customer_count}")
        

        cursor.execute("""
            SELECT 
                CASE WHEN geo_enriched = 1 THEN 'Enriched' ELSE 'Not Enriched' END as status,
                COUNT(*) as count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage
            FROM customer_enriched
            GROUP BY geo_enriched
        """)
        
        print("\n\nGeographic Enrichment Status:")
        for row in cursor.fetchall():
            print(f"  {row.status}: {row.count} ({row.percentage}%)")
        
        print(f"\n{'='*60}")
        print("Report generation completed successfully")
        print(f"{'='*60}\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error generating geographic distribution report: {e}")
        return False


if __name__ == "__main__":
    generate_geographic_distribution_report()
