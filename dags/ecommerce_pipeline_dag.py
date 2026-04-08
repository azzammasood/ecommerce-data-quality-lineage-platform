import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import pandas as pd

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Ensure data directory exists
DATA_DIR = '/opt/airflow/data'

def load_csv_to_postgres():
    """
    Simulates downloading from AWS S3 and extracting into the landing area (Postgres 'raw' schema).
    """
    # Connect directly to ecommerce DB
    db_uri = 'postgresql://ecommerce_user:ecommerce_pass@postgres:5432/ecommerce'
    engine = create_engine(db_uri)
    
    # Ensure 'raw' schema exists
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
    
    # In a real environment, you'd use an S3ToSqlOperator. 
    # Here we load our generated local CSVs.
    
    files = {
        'users': 'users.csv',
        'products': 'products.csv',
        'orders': 'orders.csv',
        'order_items': 'order_items.csv'
    }
    
    print("Pre-fetching from mock S3 landing zone...")
    
    for table_name, filename in files.items():
        file_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(file_path):
            print(f"Loading {filename} into raw.{table_name}...")
            df = pd.read_csv(file_path)
            # Drop table CASCADE to handle dbt view dependencies cleanly
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE;"))
                
            df.to_sql(
                name=table_name,
                schema='raw',
                con=engine,
                if_exists='replace',
                index=False
            )
            print(f"Successfully loaded {len(df)} rows into raw.{table_name}")
        else:
            print(f"File {file_path} not found. Skipping...")


with DAG(
    'ecommerce_data_quality_and_lineage',
    default_args=default_args,
    description='E-commerce pipeline simulating S3 Extraction, Postgres Loading, and dbt Transformations with Marquez Lineage',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 'dbt', 'openlineage'],
) as dag:

    # 1. Trigger Data Generation (Mocking external database emitting to S3)
    # The script generates CSVs locally and optionally uploads to S3
    generate_fake_data = BashOperator(
        task_id='generate_and_emit_to_s3',
        bash_command='python /opt/airflow/scripts/generate_fake_data.py',
    )

    # 2. Extract from S3 to Data Warehouse Landing Schema
    extract_to_landing = PythonOperator(
        task_id='extract_s3_to_postgres_raw',
        python_callable=load_csv_to_postgres,
    )

    # 3. Transform via dbt and Run Data Quality tests
    # Setting the project dir and profiles dir
    DBT_DIR = '/opt/airflow/dbt_ecommerce'
    
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=(
            f'export DBT_HOST=postgres && '
            f'export OPENLINEAGE_URL=$OPENLINEAGE_URL && '
            f'export OPENLINEAGE_NAMESPACE=ecommerce-data-pipeline && '
            f'cd {DBT_DIR} && '
            f'dbt-ol run --profiles-dir .'
        ),
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command=(
            f'export DBT_HOST=postgres && '
            f'export OPENLINEAGE_URL=$OPENLINEAGE_URL && '
            f'export OPENLINEAGE_NAMESPACE=ecommerce-data-pipeline && '
            f'cd {DBT_DIR} && '
            f'dbt-ol test --profiles-dir .'
        ),
    )

    # Workflow Dependencies
    generate_fake_data >> extract_to_landing >> dbt_run >> dbt_test
