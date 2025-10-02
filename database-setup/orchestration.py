import sys
import pyodbc
from pathlib import Path

# --- CONFIG ---
SERVER = "localhost"
DATABASE = "customer_warehouse"
DRIVER = "ODBC Driver 17 for SQL Server"
TRUSTED_CONNECTION = "yes"

# --- Locate SQL files ---
base_dir = Path(__file__).resolve().parent
sql_files = sorted(base_dir.glob("*.sql"))

if not sql_files:
    print(f"No .sql files found in {base_dir}")
    sys.exit(1)

print(f"Found {len(sql_files)} SQL file(s): {[f.name for f in sql_files]}")

# --- Build connection strings ---
conn_str_master = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE=master;Trusted_Connection={TRUSTED_CONNECTION};"
conn_str_db = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection={TRUSTED_CONNECTION};"

# --- Check connection to SQL Server ---
try:
    conn = pyodbc.connect(conn_str_master, timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION;")
    version = cursor.fetchone()[0]
    print(f"Connected to SQL Server: {version}")
    conn.close()
except Exception as e:
    print(f"Cannot connect to SQL Server: {e}")
    sys.exit(1)

# --- Run each SQL file ---
for idx, sql_path in enumerate(sql_files, 1):
    print(f"\nRunning {sql_path.name}...")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # crude split on GO
    sql_script = sql_script.replace("\r\n", "\n").replace("\r", "\n")
    batches = [b.strip() for b in sql_script.split("GO") if b.strip()]

    # use master for the first file, target DB afterwards
    conn_str = conn_str_master if idx == 1 else conn_str_db

    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        for batch in batches:
            cursor.execute(batch)
        conn.close()
        print(f"Finished running {sql_path.name}")
    except Exception as e:
        print(f"Failed running {sql_path.name}: {e}")
        sys.exit(1)

print("\nSetup completed successfully.")
