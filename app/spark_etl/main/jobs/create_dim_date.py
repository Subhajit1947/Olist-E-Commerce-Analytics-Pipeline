from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_dim_date_spark(spark, start_date='2016-01-01', end_date='2018-12-31'):
    """
    Create comprehensive date dimension table using PySpark
    """
    # Generate date range using sequence (more efficient)
    date_range = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
    """)
    
    dim_date = date_range.select(
        # Primary key
        col("date"),
        expr("CAST(REPLACE(CAST(date AS STRING), '-', '') AS INT) AS date_key"),
        
        # Basic date parts
        dayofmonth("date").alias("day"),
        month("date").alias("month"),
        year("date").alias("year"),
        quarter("date").alias("quarter"),
        dayofweek("date").alias("day_of_week"),  # Sunday=1, Saturday=7
        date_format("date", "EEEE").alias("day_name"),
        date_format("date", "MMMM").alias("month_name"),
        
        # Business logic
        when(dayofweek("date").isin(1, 7), True).otherwise(False).alias("weekend_flag"),
        weekofyear("date").alias("week_number"),
        date_format("date", "yyyy-MM").alias("month_year"),
        
        # Fiscal year (adjust as needed)
        year("date").alias("fiscal_year"),
        quarter("date").alias("fiscal_quarter"),
        
        # Additional flags
        when(dayofweek("date").isin(1, 7), False).otherwise(True).alias("is_weekday"),
        lit(False).alias("is_holiday")
    )
    
    return dim_date