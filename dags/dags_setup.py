from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from ETL import data_ingestion
from ETL import ETL_to_Data_Mart
from ETL import ETL_to_Data_Warehouse

default_args = {
    'owner': 'Duy Doan',
    'email': 'doanduy233@gmail.com',
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    default_args=default_args,
    dag_id='amazon_product_sales_2023',
    description='Data pipeline create the report for product sale 2023 of amazon',
    start_date=datetime(2023, 7, 1),
    schedule_interval='0 0 * * 1'
) as dag:

    task1 = PythonOperator(
        task_id='Data_Ingestion',
        python_callable=data_ingestion
    )

    task2 = PythonOperator(
        task_id='ETL_to_Data_Mart',
        python_callable=ETL_to_Data_Mart
    )
    
    task3 = PythonOperator(
        task_id='ETL_to_Data_Warehouse',
        python_callable=ETL_to_Data_Warehouse
    )

task1 >> [task2, task3]
