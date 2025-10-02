import pyodbc
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection_string():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
    )


def generate_risk_analysis_report():
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("RISK ANALYSIS REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        

        cursor.execute("""
            SELECT 
                calculated_risk,
                COUNT(*) as customer_count,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) as percentage,
                AVG(CAST(risk_score_numeric AS FLOAT)) as avg_risk_score,
                MIN(risk_score_numeric) as min_score,
                MAX(risk_score_numeric) as max_score
            FROM customer_enriched
            GROUP BY calculated_risk
            ORDER BY avg_risk_score DESC
        """)
        
        print("Risk Level Distribution:")
        total = 0
        for row in cursor.fetchall():
            total += row.customer_count
            print(f"\n  {row.calculated_risk} Risk:")
            print(f"    Customers: {row.customer_count} ({row.percentage}%)")
            print(f"    Avg Score: {row.avg_risk_score:.2f}")
            print(f"    Score Range: {row.min_score} - {row.max_score}")
        
        print(f"\nTotal Customers Analyzed: {total}")
        

        cursor.execute("""
            SELECT 
                CASE WHEN is_business = 1 THEN 'Business' ELSE 'Personal' END as customer_type,
                calculated_risk,
                COUNT(*) as count
            FROM customer_enriched
            GROUP BY is_business, calculated_risk
            ORDER BY is_business, calculated_risk
        """)
        
        print("\n\nRisk by Customer Type:")
        current_type = None
        for row in cursor.fetchall():
            if current_type != row.customer_type:
                current_type = row.customer_type
                print(f"\n  {current_type}:")
            print(f"    {row.calculated_risk} Risk: {row.count}")
        

        cursor.execute("""
            SELECT TOP 10
                customer_id,
                first_name,
                last_name,
                calculated_risk,
                risk_score_numeric,
                risk_factors
            FROM customer_enriched
            WHERE calculated_risk IN ('High', 'Medium')
            ORDER BY risk_score_numeric DESC
        """)
        
        print("\n\nTop Risk Customers:")
        for idx, row in enumerate(cursor.fetchall(), 1):
            print(f"\n  {idx}. Customer {row.customer_id} - {row.first_name} {row.last_name}")
            print(f"     Risk: {row.calculated_risk} (Score: {row.risk_score_numeric})")
            print(f"     Factors: {row.risk_factors}")
        
        print(f"\n{'='*60}")
        print("Report generation completed successfully")
        print(f"{'='*60}\n")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error generating risk analysis report: {e}")
        return False


if __name__ == "__main__":
    generate_risk_analysis_report()
