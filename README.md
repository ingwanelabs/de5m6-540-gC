# Data Enrichment & Load

This project provides a self-contained ETL + validation pipeline for ingesting, enriching, and loading data into a SQL database, with a suite of post-load quality checks.

## Contents

- `database-setup/` — SQL scripts and orchestration to create database and tables  
- `etl-pipeline/etl_pipe.py` — pipeline logic to read, transform, and load data  
- `data-validation/` — a set of validators (record count, completeness, risk distribution, geo distribution, audit trail)  
- `data/` — sample CSV files (e.g. `new_users.csv`, `update_users.csv`)  
- `output-scripts/` — (optional) scripts to extract or report on loaded data  
- `main.py` — entry point (if using combined flow)  
- `requirements.txt` — Python dependencies  
- `docker-compose.yml` — optional containerised setup  

## Prerequisites

- Python **3.8+**  
- A SQL Server (or compatible) instance and driver (e.g. “ODBC Driver 17 for SQL Server”)  
- Network access and authentication to the database  

## Setup & Run

1. **Clone & install**

    ```bash
    git clone https://github.com/LinusTML/data-enrichment_load.git
    cd data-enrichment_load
    python -m venv .venv
    source .venv/bin/activate       # or use equivalent on Windows
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

2. **Configure database connection**

    Create a `.env` file in the root:

    ```dotenv
    DB_SERVER=your_server
    DB_NAME=your_target_database
    DB_USER=your_username
    DB_PASSWORD=your_password
    DB_DRIVER=ODBC Driver 17 for SQL Server
    ```

3. **Initialize database & tables**

    Option A — run SQL scripts:

    ```bash
    sqlcmd -S "$DB_SERVER" -i database-setup/setup_db.sql
    sqlcmd -S "$DB_SERVER" -d "$DB_NAME" -i database-setup/setup_tables.sql
    ```

    Option B — use orchestration:

    ```bash
    python database-setup/orchestration.py
    ```

4. **Run ETL**

    ```bash
    python etl-pipeline/etl_pipe.py
    ```

5. **Run main**
    Choose options for ETL
    ```bash
    python main.py
    ```
   

    It reads files from `data/`, applies enrichment
