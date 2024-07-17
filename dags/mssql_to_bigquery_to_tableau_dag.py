from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohan',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mssql_to_bigquery_to_tableau',
    default_args=default_args,
    description='Extract from MSSQL, load to BigQuery, transform, and prep for Tableau',
    schedule_interval=timedelta(days=1),
)

tables = {
    "SalesOrderHeader": {
        "query": "SELECT SalesOrderID, OrderDate, CustomerID, TerritoryID, SubTotal, TaxAmt, Freight, TotalDue FROM Sales.SalesOrderHeader",
        "schema": [
            {'name': 'SalesOrderID', 'type': 'INTEGER'},
            {'name': 'OrderDate', 'type': 'TIMESTAMP'},
            {'name': 'CustomerID', 'type': 'INTEGER'},
            {'name': 'TerritoryID', 'type': 'INTEGER'},
            {'name': 'SubTotal', 'type': 'FLOAT'},
            {'name': 'TaxAmt', 'type': 'FLOAT'},
            {'name': 'Freight', 'type': 'FLOAT'},
            {'name': 'TotalDue', 'type': 'FLOAT'},
        ]
    },
    "SalesOrderDetail": {
        "query": "SELECT SalesOrderID, SalesOrderDetailID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount, LineTotal FROM Sales.SalesOrderDetail",
        "schema": [
            {'name': 'SalesOrderID', 'type': 'INTEGER'},
            {'name': 'SalesOrderDetailID', 'type': 'INTEGER'},
            {'name': 'OrderQty', 'type': 'INTEGER'},
            {'name': 'ProductID', 'type': 'INTEGER'},
            {'name': 'UnitPrice', 'type': 'FLOAT'},
            {'name': 'UnitPriceDiscount', 'type': 'FLOAT'},
            {'name': 'LineTotal', 'type': 'FLOAT'},
        ]
    },
    "SalesTerritory": {
        "query": "SELECT TerritoryID, Name, CountryRegionCode FROM Sales.SalesTerritory",
        "schema": [
            {'name': 'TerritoryID', 'type': 'INTEGER'},
            {'name': 'Name', 'type': 'STRING'},
            {'name': 'CountryRegionCode', 'type': 'STRING'},
        ]
    },
    "Product": {
        "query": "SELECT ProductID, Name, StandardCost, ListPrice, ProductSubcategoryID FROM Production.Product",
        "schema": [
            {'name': 'ProductID', 'type': 'INTEGER'},
            {'name': 'Name', 'type': 'STRING'},
            {'name': 'StandardCost', 'type': 'FLOAT'},
            {'name': 'ListPrice', 'type': 'FLOAT'},
            {'name': 'ProductSubcategoryID', 'type': 'INTEGER'},
        ]
    },
    "ProductSubcategory": {
        "query": "SELECT ProductSubcategoryID, ProductCategoryID, Name FROM Production.ProductSubcategory",
        "schema": [
            {'name': 'ProductSubcategoryID', 'type': 'INTEGER'},
            {'name': 'ProductCategoryID', 'type': 'INTEGER'},
            {'name': 'Name', 'type': 'STRING'},
        ]
    },
    "ProductCategory": {
        "query": "SELECT ProductCategoryID, Name FROM Production.ProductCategory",
        "schema": [
            {'name': 'ProductCategoryID', 'type': 'INTEGER'},
            {'name': 'Name', 'type': 'STRING'},
        ]
    }
}

# Task 1: Extract data from MSSQL and upload to GCS
with TaskGroup(group_id='extract_tasks', dag=dag) as extract_group:
    extract_tasks = []
    for table_name, details in tables.items():
        extract_task = MSSQLToGCSOperator(
            task_id=f'extract_{table_name.lower()}_to_gcs',
            mssql_conn_id='mssql_default',
            sql=details["query"],
            bucket='mssql-bigquery-etl-bucket',
            filename=f'{table_name.lower()}.csv',
            export_format='CSV',
            field_delimiter=',',
            gcp_conn_id='google_cloud_default',
            dag=dag,
        )
        extract_tasks.append(extract_task)

# Task 2: Load data from GCS to BigQuery
with TaskGroup(group_id='load_tasks', dag=dag) as load_group:
    load_tasks = []
    for table_name, details in tables.items():
        load_task = GCSToBigQueryOperator(
            task_id=f'load_{table_name.lower()}_to_bigquery',
            bucket='mssql-bigquery-etl-bucket',
            source_objects=[f'{table_name.lower()}.csv'],
            destination_project_dataset_table=f'analytics-390815.adventure_works.{table_name.lower()}',
            schema_fields=details["schema"],
            write_disposition='WRITE_TRUNCATE',
            skip_leading_rows=1,
            source_format='CSV',
            field_delimiter=',',
            gcp_conn_id='google_cloud_default',
            dag=dag,
        )
        load_tasks.append(load_task)

# Task 3: Transform data in BigQuery
transform_data = BigQueryExecuteQueryOperator(
    task_id='transform_data',
    sql="""
    SELECT
        s.SalesOrderDetailID AS sales_line_id,
        s.SalesOrderID AS sales_order_id,
        so.CustomerID AS customer_id,
        t.Name AS territory_name,
        t.CountryRegionCode AS country_code,
        so.OrderDate AS order_date,
        ROUND(s.UnitPrice*(1- s.UnitPriceDiscount), 2) AS unit_price,
        s.OrderQty AS quantity,
        ROUND(s.LineTotal, 2) AS amount,
        s.ProductID,
        p.Name AS product_name,
        ROUND(p.StandardCost, 2) AS cost,
        ROUND(p.ListPrice, 2) AS list_price,
        sc.Name AS subcategory,
        c.Name AS category
    FROM
        `analytics-390815.adventure_works.salesorderdetail` s
    LEFT JOIN
        `analytics-390815.adventure_works.product` p
    ON
        s.ProductID = p.ProductID
    LEFT JOIN 
        `analytics-390815.adventure_works.productsubcategory` sc
    ON
        p.ProductSubcategoryID = sc.ProductSubcategoryID
    LEFT JOIN
        `analytics-390815.adventure_works.productcategory` c 
    ON
        sc.ProductCategoryID = c.ProductCategoryID
    LEFT JOIN
        `analytics-390815.adventure_works.salesorderheader` so 
    ON
        s.SalesOrderID = so.SalesOrderID
    LEFT JOIN
        `analytics-390815.adventure_works.salesterritory` t
    ON
        so.TerritoryID = t.TerritoryID
    """,
    use_legacy_sql=False,
    destination_dataset_table='analytics-390815.adventure_works.transformed_sales_data',
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Set task dependencies so that loading starts after all extractions, and transformation after all loading
extract_group >> load_group >> transform_data