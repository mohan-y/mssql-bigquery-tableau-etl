# MSSQL to BigQuery ETL

This project contains an ETL pipeline using Apache Airflow to extract data from MSSQL, load it to Google Cloud Storage (GCS), and then load it to Google BigQuery. Finally, the data is transformed and made ready for use in Tableau.

## Project Structure

```
mssql-bigquery-tableau-etl/
│
├── dags/
│   └── mssql_to_bigquery_to_tableau_dag.py   # DAG for the ETL process
│
├── airflow_home/
│   ├── airflow.cfg                          # Airflow configuration file
│   ├── airflow-webserver.pid                # Airflow webserver PID
│   └── airflow.db                           # Airflow SQLite database (not to be committed)
│
├── .gitignore                               # Git ignore file
├── analytics-390815-2d2332940f24.json       # Google Cloud service account key (not to be committed)
└── README.md                                # Project README file
```

## Setup

1. **Clone the repository**:
    ```sh
    git clone https://github.com/your_username/mssql-bigquery-tableau-etl.git
    cd mssql-bigquery-tableau-etl
    ```

2. **Set up a virtual environment**:
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

4. **Configure Airflow**:
    - Ensure your Airflow \`airflow.cfg\` is properly configured.
    - Set the \`AIRFLOW_HOME\` environment variable to point to the \`airflow_home\` directory.

5. **Run Airflow**:
    ```sh
    airflow db init
    airflow webserver --port 8080
    airflow scheduler
    ```

## Usage

- **Trigger the DAG**:
  - Access the Airflow web interface at `http://localhost:8080`.
  - Enable and trigger the `mssql_to_bigquery_to_tableau` DAG.
