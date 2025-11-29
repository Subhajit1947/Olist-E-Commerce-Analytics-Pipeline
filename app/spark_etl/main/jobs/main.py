import sys
import os
from dotenv import load_dotenv

load_dotenv("/opt/app/.env")
# Add the base path to Python path
sys.path.insert(0, '/opt/app/spark_etl')

from main.utils.spark_session import create_spark_session
from main.utils.logging_config import logger
import os
from main.read.s3_reader import S3Reader
from main.utils.s3_client_object import S3ClientProvider
from main.jobs.create_dim_date import create_dim_date_spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark=create_spark_session()
logger.info("*******************spark session created*************************")

logger.info("*******reading bucket raw directory***********")
s3_client_provider = S3ClientProvider(aws_access_key=os.getenv('AWS_ACCESS_KEY'),aws_secret_key=os.getenv('AWS_SECRET_KEY'))
s3_client = s3_client_provider.get_client()

try:
    s3_reder=S3Reader()
    folder_path=os.getenv("S3_DIRECTORY")
    s3_absolute_path=s3_reder.list_files(
                            s3_client,
                            os.getenv("S3_BUCKET"),
                            folder_path
                        )
    if not s3_absolute_path:
        logger.info(f"No data avaliable at {folder_path}")
        raise Exception("No Data avaliable to process")
    else:
        logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_path)

except Exception as e:
    logger.error("Exited with error- %s",e)
    raise e

logger.info("*******start creating dataframe from s3 raw directory***********")
dataframes={}
for s3_path in s3_absolute_path:
    if s3_path.endswith(".csv"):
        new_s3_path=s3_path.replace("s3://","s3a://")
        df=spark.read.format("csv")\
                        .option("header","true")\
                        .option("inferSchema","true")\
                        .load(new_s3_path)
        table_name=s3_path.split("/")[-1].split("_")[1]
        dataframes[table_name]=df


if "customers" in dataframes.keys():
    customers_df=dataframes["customers"]
if "orderitems" in dataframes.keys():
    orderitems_df=dataframes["orderitems"].drop(col("shipping_limit_date"))
if "orderpayments" in dataframes.keys():
    orderpayments_df=dataframes["orderpayments"]
if "orderreviews" in dataframes.keys():
    orderreviews_df=dataframes["orderreviews"]
if "orders" in dataframes.keys():
    orders_df=dataframes["orders"]
if "products" in dataframes.keys():
    products_df=dataframes["products"]
if "sellers" in dataframes.keys():
    sellers_df=dataframes["sellers"]
if "category" in dataframes.keys():
    category_df=dataframes["category"]


logger.info("*******all dataframe are created successfully***********")


logger.info("*******creating product dimension table***********")

products_dim_df=products_df.join(category_df,products_df["product_category_name"]==category_df["product_category_name"],"inner")\
    .drop(category_df["product_category_name"])
logger.info("*******success fully created product dimension table***********")
logger.info("*******creating date dimension table***********")
date_dim_df=create_dim_date_spark(spark)
logger.info("*******successfully created date dimension table***********")
logger.info("*******creating order item fact table***********")
order_item_window=Window.partitionBy("order_id","product_id","seller_id")
orderitems_df=orderitems_df.withColumn("count",count("*").over(order_item_window)).drop(col("order_item_id")).distinct()\
    .withColumn("revenue",(col("price")+col("freight_value"))*col("count"))


orderitems_orders_df=orderitems_df.join(orders_df,orderitems_df["order_id"]==orders_df["order_id"])\
    .withColumn(
        "order_item_key", 
        monotonically_increasing_id()
    )\
    .withColumn(
        "delivery_days",
        datediff(
            to_date(col("order_delivered_customer_date")),
            to_date(col("order_purchase_timestamp"))
        )
    )\
    .withColumn(
        "delivery_delay",
        datediff(
            to_date(col("order_delivered_customer_date")),
            to_date(col("order_estimated_delivery_date"))
        )
    )\
    .select(
        col("order_item_key"),
        orderitems_df["order_id"],
        col("product_id"),
        col("seller_id"),
        col("price"),
        col("freight_value"),
        col("count"),
        col("revenue"),
        col("customer_id"),
        col("order_status"),
        col("delivery_days"),
        col("delivery_delay"),
        to_date(col("order_purchase_timestamp")).alias("order_date"),
        to_date(col("order_delivered_customer_date")).alias("delivery_date")
    )

orderitems_orders_odate_df=orderitems_orders_df.join(date_dim_df.select("date", "date_key"),orderitems_orders_df["order_date"]==date_dim_df["date"],'inner')\
    .drop(date_dim_df["date"])\
    .withColumn("order_date_key",col("date_key"))\
    .drop(col("date_key"))

main_fact_df=orderitems_orders_odate_df.join(date_dim_df.select("date", "date_key"),orderitems_orders_odate_df["delivery_date"]==date_dim_df["date"],'inner')\
    .drop(date_dim_df["date"])\
    .withColumn("delivery_date_key",col("date_key"))\
    .drop(col("date_key"))

logger.info("*******successfully created order item fact table***********")
logger.info("*******try to store fact table in s3 bucket***********")
processed_directory=os.getenv("S3_PROCESSED_DIRECTORY")
bucket_name=os.getenv("S3_BUCKET")

main_fact_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/facttable/")
logger.info("*******successfully store fact table in s3 bucket***********")
logger.info("*******try to store product_dim table in s3 bucket***********")
products_dim_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/product_dim/")
logger.info("*******successfully store product_dim table in s3 bucket***********")
logger.info("*******try to store date_dim table in s3 bucket***********")
date_dim_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/date_dim/")
logger.info("*******successfully store date_dim table in s3 bucket***********")
logger.info("*******try to store customers_df table in s3 bucket***********")
customers_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/customers_dim/")
logger.info("*******successfully store customers_df table in s3 bucket***********")
logger.info("*******try to store sellers_df table in s3 bucket***********")
sellers_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/sellers_dim/")
logger.info("*******successfully store sellers_df table in s3 bucket***********")
logger.info("*******try to store orders_df table in s3 bucket***********")
orders_df.write \
    .mode("overwrite") \
    .parquet(f"s3a://{bucket_name}/{processed_directory}/orders_dim/")

logger.info("*******successfully store orders_df table in s3 bucket***********")





