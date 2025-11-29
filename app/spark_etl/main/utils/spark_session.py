# spark_etl/jobs/utils/spark_session.py
from pyspark.sql import SparkSession
import yaml
import os
from dotenv import load_dotenv

load_dotenv("/opt/app/.env")

def create_spark_session(app_name="OlistETL"):
    """Create Spark session with S3 configuration"""
    
    # Load configuration
    with open('/opt/app/spark_etl/config/spark_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    # Build Spark session
    # spark_builder = SparkSession.builder.appName(app_name)
    
    # # Add S3 configuration
    # spark_builder.config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY'))
    # spark_builder.config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_KEY'))
    # spark_builder.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

    spark_builder = SparkSession.builder.appName(app_name)
    
    # Add S3 configuration - COMPLETE configuration
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    spark_builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    spark_builder.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    spark_builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark_builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    spark_builder.config("spark.hadoop.fs.s3a.path.style.access", "false")

    
    # Add additional configs
    for key, value in config['spark']['config'].items():
        spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark