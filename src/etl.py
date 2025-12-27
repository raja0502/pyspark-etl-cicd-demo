from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def run_etl():
    spark = SparkSession.builder.appName("ETL-Demo").getOrCreate()
    
    # Extract
    df = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
    
    # Transform: Clean price (remove $), fix customer_id, parse dates
    df_clean = (df.withColumn("price", regexp_replace(col("price"), "\\$", "").cast("double"))
                 .withColumn("customer_id", 
                    when(col("customer_id").rlike("CUST[0-9]+"), col("customer_id"))
                    .when(col("customer_id").rlike("^[0-9]+$"), concat(lit("CUST_"), col("customer_id")))
                    .otherwise(col("customer_id")))
                 .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
                 .filter(col("customer_id") != "TEST999")  # Remove test data
                )
    
    # Load to processed folder
    df_clean.coalesce(1).write.mode("overwrite").parquet("data/processed/orders_clean.parquet")
    print(f"ETL complete! Processed {df_clean.count()} records")
    spark.stop()

if __name__ == "__main__":
    run_etl()
