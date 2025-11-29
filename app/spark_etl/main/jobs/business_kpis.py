import sys
import os
from dotenv import load_dotenv

load_dotenv("/opt/app/.env")
# Add the base path to Python path
sys.path.insert(0, '/opt/app/spark_etl')

from main.utils.spark_session import create_spark_session
from main.utils.logging_config import logger
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark=create_spark_session()
logger.info("*******************spark session created*************************")
logger.info("*******create customers_df from s3 customers_dim table***********")
processed_directory=os.getenv("S3_PROCESSED_DIRECTORY")
bucket_name=os.getenv("S3_BUCKET")
customers_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/customers_dim/")
logger.info("*******successfully create customers_df from s3 customers_dim table***********")
logger.info("*******create orders_df from s3 customers_dim table***********")
orders_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/orders_dim/")
logger.info("*******successfully create orders_df from s3 customers_dim table***********")

logger.info("*******create products_df from s3 customers_dim table***********")
products_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/product_dim/")
logger.info("*******successfully create products_df from s3 customers_dim table***********")

logger.info("*******create sellers_df from s3 customers_dim table***********")
sellers_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/sellers_dim/")
logger.info("*******successfully create sellers_df from s3 customers_dim table***********")

logger.info("*******create date_dim_df from s3 customers_dim table***********")
date_dim_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/date_dim/")
logger.info("*******successfully create date_dim_df from s3 customers_dim table***********")

logger.info("*******create main_fact_df from s3 customers_dim table***********")
main_fact_df=spark.read.parquet(f"s3a://{bucket_name}/{processed_directory}/facttable/")
logger.info("*******successfully create main_fact_df from s3 customers_dim table***********")
logger.info("*******all dataframe are created successfully***********")

logger.info("***************Q1. How much revenue is generated per month and per year overall and by state?***************​")
main_fact_date_df=main_fact_df.join(date_dim_df,main_fact_df["order_date_key"]==date_dim_df["date_key"],"inner").drop(date_dim_df["date_key"])

main_fact_date_cust_df=main_fact_date_df.join(customers_df,main_fact_date_df["customer_id"]==customers_df["customer_id"]).drop(customers_df["customer_id"])
main_fact_date_cust_df.groupBy(col("customer_state"),col("year"),col("month_name"))\
    .agg(sum(col("revenue")).alias("total_revenue"))\
    .select(col("customer_state").alias("state"),col("year"),col("month_name"),col("total_revenue"))\
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3a://{bucket_name}/business_answer/Q1/")

logger.info("************Q1. successfully solve check Q1 folder***********")

logger.info("***************Q2. Which product categories generate the highest total revenue and quantity sold?​***************​")
# product_category_name_english
main_fact_df.join(products_df,main_fact_df["product_id"]==products_df["product_id"],"inner").drop(products_df["product_id"])\
    .groupBy(col("product_category_name_english"))\
    .agg(sum(col("count")).alias("quantity_sold"),round(sum(col("revenue")),2).alias("total_revenue"))\
    .select(
        col("product_category_name_english"),
        col("quantity_sold"),
        col("total_revenue")
    ).orderBy(col("total_revenue").desc()).limit(2).show()

logger.info("************Q2. successfully solve check log file***********")
logger.info("************Q3. Which sellers contribute the most revenue in each state or city?​***********")

window_state_rank = Window.partitionBy("seller_state").orderBy(col("total_revenue").desc())
window_seller_revenue = Window.partitionBy("seller_id")

main_fact_df.join(sellers_df,main_fact_df["seller_id"]==sellers_df["seller_id"],"inner")\
    .drop(sellers_df["seller_id"])\
    .withColumn("total_revenue",sum(col("revenue")).over(window_seller_revenue))\
    .withColumn("rank",rank().over(window_state_rank))\
    .filter(col("rank")==1)\
    .select(
        col("seller_state"),
        col("seller_id"),
        col("total_revenue")
    ).distinct()\
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3a://{bucket_name}/business_answer/Q3/")
logger.info("************Q3. successfully solve check Q3 folder***********")