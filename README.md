# Northwind ETL with Airflow and PostgreSQL

This project processes the Northwind CSV files using Python and Airflow, cleans and validates the data, creates analytical tables, saves versioned artifacts, and loads the results into PostgreSQL.

## Architecture

- `IN/`: input CSV files.
- `etl_pipeline.py`: ETL, validation, transformation, output, and PostgreSQL loading logic.
- `dags/northwind_postgres_dag.py`: Airflow DAG definition.
- `sql/northwind_postgres_schema.sql`: PostgreSQL table definitions.
- `sql/northwind_queries.sql`: analytical SQL queries.
- `output/`: versioned output and Airflow staging files.
- `start_airflow.sh`: Bash script for starting, stopping, and checking Airflow.

The DAG runs these tasks in order:

1. `check_input_directory`: verifies the required files.
2. `extract_raw_files`: reads and validates the five CSV files.
3. `transform_and_validate`: cleans values, detects invalid rows, joins datasets, and creates analytical tables.
4. `save_output_files`: saves versioned CSV artifacts.
5. `load_postgres`: inserts transformed data and audit records into PostgreSQL.

Airflow runs inside Ubuntu on WSL2. PostgreSQL runs on Windows and is accessed from WSL through the configured gateway address.

## Requirements

```bash
python -m pip install "apache-airflow==2.9.3" pandas sqlalchemy psycopg2-binary
```

Required components:

- Windows 10 or Windows 11 with WSL2.
- Ubuntu 22.04 in WSL2.
- Python 3.10 or newer inside Ubuntu.
- Apache Airflow 2.9.3 in a Linux virtual environment.
- PostgreSQL running on Windows.
- A PostgreSQL database named `northwind`.

The virtual environment must be created inside the Linux filesystem, for example `/home/ubuntu/northwind_project/.venv`. Do not run Airflow with a Windows virtual environment under `/mnt/c`.

## PostgreSQL Configuration

The ETL reads `PG_DSN` when it is defined. Otherwise, it uses the default DSN in `etl_pipeline.py`.

```bash
export PG_DSN="postgresql+psycopg2://user:password@host:5432/northwind"
```

When PostgreSQL runs on Windows and Airflow runs in WSL2, PostgreSQL must allow the WSL subnet in `pg_hba.conf`, and Windows Firewall must allow inbound TCP traffic on port `5432` from that subnet.

## Initial Setup

```bash
cd /home/ubuntu/northwind_project
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "apache-airflow==2.9.3" pandas sqlalchemy psycopg2-binary
```

Create the database tables by running `sql/northwind_postgres_schema.sql` against the `northwind` database if they do not exist yet.

## Start Airflow with Bash

Make the script executable once:

```bash
chmod +x start_airflow.sh
```

Start Airflow:

```bash
./start_airflow.sh start
```

The script sets `AIRFLOW_HOME`, selects the project DAG folder, disables example DAGs, uses `SequentialExecutor`, runs `airflow db migrate`, and starts the scheduler and webserver in the background. Logs are written to `logs/` and process IDs to `.airflow-pids/`.

Open the UI at `http://localhost:8080`.

Useful commands:

```bash
./start_airflow.sh status
./start_airflow.sh stop
tail -f logs/scheduler.log
tail -f logs/webserver.log
```

## Airflow User

Create an administrator account once, if necessary:

```bash
airflow users create \
	--username admin \
	--firstname Admin \
	--lastname User \
	--role Admin \
	--email admin@example.com
```

Airflow asks for the password interactively. Do not store the password in this README or in the Bash script.

## Run the DAG

1. Run `./start_airflow.sh start`.
2. Open `http://localhost:8080` and sign in.
3. Enable `northwind_postgres_pipeline`.
4. Trigger a manual run.
5. Review each task in the Graph or Grid view.

## Run Without Airflow

```bash
python etl_pipeline.py
```

## Data Quality Rules

- expected row-count validation for all five source files;
- explicit handling of `order-details.csv`;
- cleanup of literal `NULL` values;
- detection of country values that resemble postal codes;
- `batch_id` and execution timestamps;
- an `audit_invalid_rows` table;
- versioned output files;
- structured logging.

## PostgreSQL Tables

- `fact_orders`
- `agg_sales_by_country`
- `agg_sales_by_category`
- `audit_invalid_rows`

The schema also contains raw and cleaned tables used by the pipeline.

## Troubleshooting

### `python: command not found`

Activate the Linux virtual environment:

```bash
source /home/ubuntu/northwind_project/.venv/bin/activate
```

### `connection refused` on port `5432`

Check that PostgreSQL is running on Windows, that port `5432` is listening, and that `pg_hba.conf` and Windows Firewall allow connections from WSL2.

### `Engine object has no attribute cursor`

Use the current `etl_pipeline.py`, which uses SQLAlchemy connections for PostgreSQL reads and inserts.

### A task is marked as a zombie

Run Airflow from the native Linux project and virtual environment. Avoid running the Airflow runtime from `/mnt/c` or a synchronized OneDrive folder.
