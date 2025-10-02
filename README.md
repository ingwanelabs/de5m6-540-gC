# Data Enrichment & Load Project

A Python-based ETL pipeline for data enrichment with automated database setup, user data processing, and comprehensive validation capabilities.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Testing](#testing)

## Project Overview

This project implements a complete data pipeline solution for user data enrichment and loading, featuring:

- **Database Setup**: SQL scripts for database and table initialization
- **ETL Pipeline**: Automated extract, transform, and load operations for user data
- **Data Validation**: Multi-layered validation including record counts, completeness checks, risk distribution analysis, geographic distribution, and audit trail verification
- **User Data Management**: Handles new user creation and existing user updates

## Project Structure

```
data-enrichment_load/
│
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── .env                              # Environment variables (not tracked)
├── .gitignore                        # Git ignore rules
│
├── database-setup/                   # Database initialization
│   ├── orchestration.py             # Database setup orchestration
│   ├── setup_db.sql                 # Database creation script
│   └── setup_tables.sql             # Table schema definitions
│
├── etl-pipeline/                     # ETL processing
│   └── etl_pipe.py                  # Main ETL pipeline logic
│
├── data-validation/                  # Data quality validation
│   ├── validate_recordCount.py      # Record count validation
│   ├── validate_completness.py      # Data completeness checks
│   ├── validate_riskDistribution.py # Risk profile distribution
│   ├── validate_geoDistribution.py  # Geographic distribution analysis
│   └── validate_auditTrailVerification.py # Audit trail integrity
│
├── data/                             # Data files
│   ├── new_users.csv                # New user records for loading
│   └── update_users.csv             # User update records
│
├── output-scripts/                   # Output and reporting scripts
│
└── tests/                            # Unit and integration tests
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- SQL Server or compatible ODBC database
- ODBC Driver 17 for SQL Server (or appropriate driver for your database)

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd data-enrichment_load
```

### 2. Create Virtual Environment

**Windows (PowerShell):**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Linux/Mac:**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Create a `.env` file in the project root with your database credentials:

```env
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 17 for SQL Server
```

### Database Setup

1. Execute database creation script:

   ```bash
   sqlcmd -S your_server -i database-setup/setup_db.sql
   ```
2. Create tables:

   ```bash
   sqlcmd -S your_server -d your_database -i database-setup/setup_tables.sql
   ```

Alternatively, run the orchestration script:

```bash
python database-setup/orchestration.py
```

## Usage

### Running the ETL Pipeline

Execute the main ETL pipeline to process user data:

```bash
python etl-pipeline/etl_pipe.py
```

The pipeline will:

1. Extract data from CSV files in the `data/` directory
2. Transform and enrich user records
3. Load processed data into the database

### Data Validation

Run validation checks to ensure data quality:

```bash
# Validate record counts
python data-validation/validate_recordCount.py

# Check data completeness
python data-validation/validate_completness.py

# Analyze risk distribution
python data-validation/validate_riskDistribution.py

# Verify geographic distribution
python data-validation/validate_geoDistribution.py

# Audit trail verification
python data-validation/validate_auditTrailVerification.py
```

## Testing

Run the test suite to verify functionality:

```bash

```

## Development Workflow

1. Activate the virtual environment
2. Make code changes in the appropriate module
3. Run tests to verify changes
4. Execute the ETL pipeline
5. Run validation scripts to ensure data quality

## Dependencies

Core dependencies:

- `pandas` - Data manipulation and analysis
- `pyodbc` - ODBC database connectivity
- `sqlalchemy` - SQL toolkit and ORM
- `pytest` - Testing framework
- `python-dotenv` - Environment variable management

See `requirements.txt` for complete dependency list.
