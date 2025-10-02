import pyodbc
import pandas as pd
from datetime import datetime
import uuid
from typing import Dict
from pathlib import Path


class DatabaseLoader:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.batch_id = str(uuid.uuid4())
        
    def load_data(self, df: pd.DataFrame) -> Dict:
        start_time = datetime.now()
        results = {
            'batch_id': self.batch_id,
            'total_records': len(df),
            'successful_inserts': 0,
            'successful_updates': 0,
            'failed_records': 0,
            'errors': [],
            'processing_time': 0
        }
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            for index, row in df.iterrows():
                try:
                    check_sql = "SELECT COUNT(*) FROM customer_enriched WHERE customer_id = ?"
                    cursor.execute(check_sql, row['customer_id'])
                    exists = cursor.fetchone()[0] > 0
                    if exists:
                        self._update_record(cursor, row)
                        results['successful_updates'] += 1
                    else:
                        self._insert_record(cursor, row)
                        results['successful_inserts'] += 1
                except Exception as e:
                    error_msg = f"customer {row['customer_id']}: {str(e)}"
                    results['errors'].append(error_msg)
                    results['failed_records'] += 1       
            conn.commit()
            end_time = datetime.now()
            results['processing_time'] = (end_time - start_time).total_seconds()
            self._log_audit(cursor, start_time, end_time, results)
            conn.commit()
            conn.close()
            print(f"Completed {results['successful_inserts']} inserts, {results['successful_updates']} updates")
        except Exception as e:
            results['errors'].append(f"error: {str(e)}")
            print(f"error: {e}")
        return results
    
    def _update_record(self, cursor, row):
        update_sql = """
        UPDATE customer_enriched SET
            first_name = ?, last_name = ?, email = ?, phone = ?, postcode = ?,
            region = ?, country = ?, district = ?, longitude = ?, latitude = ?, geo_enriched = ?,
            company = ?, company_size = ?, industry = ?, annual_revenue = ?, is_business = ?,
            calculated_risk = ?, risk_score_numeric = ?, risk_factors = ?,
            status = ?, processed_date = ?, data_source = ?, enrichment_status = ?,
            modified_date = GETDATE()
        WHERE customer_id = ?
        """
        
        cursor.execute(update_sql, (
            row['first_name'], row['last_name'], row['email'],
            row['phone'], row['postcode'],
            row['region'], row['country'], row['district'],
            row['longitude'], row['latitude'], row['geo_enriched'],
            row['company'], row['company_size'], row['industry'],
            row['annual_revenue'], row['is_business'],
            row['calculated_risk'], row['risk_score_numeric'], row['risk_factors'],
            row['status'], row['processed_date'], row['data_source'],
            row['enrichment_status'], row['customer_id']
        ))
    
    def _insert_record(self, cursor, row):
        insert_sql = """
        INSERT INTO customer_enriched (
            customer_id, first_name, last_name, email, phone, postcode,
            region, country, district, longitude, latitude, geo_enriched,
            company, company_size, industry, annual_revenue, is_business,
            calculated_risk, risk_score_numeric, risk_factors,
            status, processed_date, data_source, enrichment_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(insert_sql, (
            row['customer_id'], row['first_name'], row['last_name'],
            row['email'], row['phone'], row['postcode'],
            row['region'], row['country'], row['district'],
            row['longitude'], row['latitude'], row['geo_enriched'],
            row['company'], row['company_size'], row['industry'],
            row['annual_revenue'], row['is_business'],
            row['calculated_risk'], row['risk_score_numeric'], row['risk_factors'],
            row['status'], row['processed_date'], row['data_source'],
            row['enrichment_status']
        ))
    
    def _log_audit(self, cursor, start_time, end_time, results):
        audit_sql = """
        INSERT INTO enrichment_audit (
            batch_id, operation_type, records_processed, records_successful, 
            records_failed, processing_start, processing_end, error_message, pipeline_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        error_summary = '; '.join(results['errors'][:5]) if results['errors'] else None
        successful = results['successful_inserts'] + results['successful_updates']
        cursor.execute(audit_sql, (
            self.batch_id,
            'UPSERT',
            results['total_records'],
            successful,
            results['failed_records'],
            start_time,
            end_time,
            error_summary,
            'v1.0'
        ))

def load_csv(csv_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} records from {Path(csv_path).name}")
        return df
    except Exception as e:
        print(f"Failed to load CSV: {e}")
        return None

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['data_source'] = 'ETL_Pipeline_v1'
    if 'enrichment_status' not in df.columns:
        df['enrichment_status'] = 'Fully Enriched'
    if 'geo_enriched' in df.columns:
        df['geo_enriched'] = df['geo_enriched'].astype(int)
    if 'is_business' in df.columns:
        df['is_business'] = df['is_business'].astype(int)
    return df


def insert_data(connection_string: str, csv_path: str) -> Dict:
    df = load_csv(csv_path)
    if df is None:
        return {'success': False, 'error': 'Failed to load CSV'}
    df = prepare_data(df)
    loader = DatabaseLoader(connection_string)
    results = loader.load_data(df)
    results['success'] = results['failed_records'] == 0
    return results

def upsert_data(connection_string: str, csv_path: str) -> Dict:
    return insert_data(connection_string, csv_path)
