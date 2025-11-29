from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # Consider enabling for production
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Add timeout
}

def check_spark_connection():
    """Optional: Check if Spark connection is available"""
    # You can implement connection validation here
    logging.info("Spark connection check would go here")

with DAG(
    'olist_business',
    default_args=default_args,
    description='Olist ETL Pipeline with Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['olist', 'spark', 'etl'],
) as dag:

    start = DummyOperator(task_id='start')
    check_connection = PythonOperator(
        task_id='check_spark_connection',
        python_callable=check_spark_connection
    )
    # spark job sumit olist ecommarce
    spark_job_ecommarce = SparkSubmitOperator(
        task_id='run_spark-submit',
        application='/opt/app/spark_etl/main/jobs/main.py',
        conn_id='spark_default',
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf={
            'spark.jars.ivy': '/tmp/.ivy2'
        } 
    )
    
    # business question
    # spark_kpi_job = SparkSubmitOperator(
    #     task_id='run_business_kpis',
    #     application='/opt/app/spark_etl/main/jobs/business_kpis.py',
    #     conn_id='spark_default',
    #     packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
    #     conf={
    #         'spark.jars.ivy': '/tmp/.ivy2'
    #     } 
    # )
    
    end = DummyOperator(task_id='end')
    start >> check_connection >> spark_job_ecommarce >> end
    # business question dependency portion
    # start >> check_connection >>spark_job_ecommarce>> spark_kpi_job >> end