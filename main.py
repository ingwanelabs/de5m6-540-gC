import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import subprocess

sys.path.append(os.path.join(os.path.dirname(__file__), 'etl-pipeline'))
from etl_pipe import insert_data, upsert_data

sys.path.append(os.path.join(os.path.dirname(__file__), 'data-validation'))
from validate_recordCount import validate_record_count
from validate_completness import validate_completeness
from validate_riskDistribution import validate_risk_distribution
from validate_geoDistribution import validate_geo_distribution
from validate_auditTrailVerification import validate_audit_trail

sys.path.append(os.path.join(os.path.dirname(__file__), 'output-scripts'))
from report_customer_demographics import generate_customer_demographics_report
from report_risk_analysis import generate_risk_analysis_report
from report_geographic_distribution import generate_geographic_distribution_report
from report_data_quality import generate_data_quality_report
from report_enrichment_status import generate_enrichment_status_report

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


def list_data_files():
    data_dir = Path('data')
    if not data_dir.exists():
        print("Data folder not found")
        return []
    
    csv_files = list(data_dir.glob('*.csv'))
    return csv_files


def display_files(files):
    print("\n~Available Data Files:~")
    for idx, file in enumerate(files, 1):
        print(f"{idx}. {file.name}")
    print()


def select_file(files):
    while True:
        try:
            choice = input("Select file number (or 0 to cancel): ")
            idx = int(choice)
            if idx == 0:
                return None
            if 1 <= idx <= len(files):
                return files[idx - 1]
            print("Invalid selection")
        except ValueError:
            print("Please enter a number")


def show_menu():
    print("\n~Main Menu~")
    print("1. Select data file")
    print("2. Generate reports")
    print("3. Exit")
    print()


def show_file_options(file_path):
    print(f"\n~Options for {file_path.name}~")
    print("1. Run validation")
    print("2. Load to database")
    print("3. Back to main menu")
    print()


def run_validation(file_path):
    print(f"\n~ Running Validation on {file_path.name} ~")
    
    validations = {
        1: ('Record Count', validate_record_count),
        2: ('Completeness', validate_completeness),
        3: ('Risk Distribution', validate_risk_distribution),
        4: ('Geographic Distribution', validate_geo_distribution),
        5: ('Audit Trail Verification', validate_audit_trail)
    }

    print("\nAvailable validations:")
    for idx, (name, _) in validations.items():
        print(f"{idx}. {name}")
    print(f"{len(validations) + 1}. Run all validations")
    print("0. Back to menu")
    
    try:
        choice = int(input("\nSelect validation: "))
        if choice == 0:
            return
        elif choice == len(validations) + 1:
            print("\nRunning all validations...")
            for name, func in validations.values():
                print(f"\n{'='*50}")
                func(str(file_path))
            print(f"\n{'='*50}")
            print("All validations completed")
        elif choice in validations:
            name, func = validations[choice]
            func(str(file_path))
        else:
            print("Invalid selection")
    except ValueError:
        print("Invalid input")


def load_to_database(file_path):
    print(f"\n~ Loading {file_path.name} to Database ~")
    try:
        connection_string = get_connection_string()
        print("\nExecuting smart load (automatic insert/update)...")
        print("Analyzing data and determining operations...")
        
        results = upsert_data(connection_string, str(file_path))

        print("\n~ Results ~")
        print(f"Total records: {results.get('total_records', 0)}")
        print(f"New records inserted: {results.get('successful_inserts', 0)}")
        print(f"Existing records updated: {results.get('successful_updates', 0)}")
        print(f"Failed records: {results.get('failed_records', 0)}")
        print(f"Processing time: {results.get('processing_time', 0):.2f}s")
        print(f"Batch ID: {results.get('batch_id', 'N/A')}")
        
        if results.get('errors'):
            print("\nErrors:")
            for error in results['errors'][:5]:
                print(f"  - {error}")
        
        if results.get('success'):
            print("\nLoad completed successfully")
            print("All operations logged to enrichment_audit table")
        else:
            print("\nLoad completed with errors")
            
    except Exception as e:
        print(f"Error: {e}")


def generate_reports():
    print("\n~ Generate Business Intelligence Reports ~")
    
    reports = {
        1: ('Customer Demographics Report', generate_customer_demographics_report),
        2: ('Risk Analysis Report', generate_risk_analysis_report),
        3: ('Geographic Distribution Report', generate_geographic_distribution_report),
        4: ('Data Quality Summary Report', generate_data_quality_report),
        5: ('Enrichment Status Report', generate_enrichment_status_report)
    }
    
    print("\nAvailable reports:")
    for idx, (name, _) in reports.items():
        print(f"{idx}. {name}")
    print(f"{len(reports) + 1}. Generate all reports")
    print("0. Back to menu")
    
    try:
        choice = int(input("\nSelect report: "))
        if choice == 0:
            return
        elif choice == len(reports) + 1:
            print("\nGenerating all reports...")
            for name, func in reports.values():
                print(f"\n{'='*60}")
                func()
            print(f"\n{'='*60}")
            print("All reports generated successfully")
        elif choice in reports:
            name, func = reports[choice]
            func()
        else:
            print("Invalid selection")
    except ValueError:
        print("Invalid input")


def main():
    print("="*50)
    print("DATA ENRICHMENT LOAD - ETL PIPELINE")
    print("="*50)
    while True:
        show_menu()
        try:
            choice = input("Select option: ")
            if choice == '1':
                files = list_data_files()
                if files:
                    display_files(files)
                    selected_file = select_file(files)
                    if selected_file:
                        print(f"\nSelected: {selected_file.name}")
                        while True:
                            show_file_options(selected_file)
                            file_choice = input("Select option: ")
                            
                            if file_choice == '1':
                                run_validation(selected_file)
                            elif file_choice == '2':
                                load_to_database(selected_file)
                            elif file_choice == '3':
                                break
                            else:
                                print("Invalid option")
                else:
                    print("No CSV files found in data folder")
            elif choice == '2':
                generate_reports()
            elif choice == '3':
                print("\nExiting...")
                break
            else:
                print("Invalid option")               
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()