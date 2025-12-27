from pyspark.sql import SparkSession
from pyspark.testing.pytestutils import assert_pyspark_df_equal
from src.etl import run_etl
import os

def test_etl_pipeline():
    spark = SparkSession.builder.appName("Test").getOrCreate()
    
    # Create test data
    test_data = [("1", "123", "Laptop", "$100", "1", "2023-01-01", "North")]
    df_test = spark.createDataFrame(test_data, 
        ["order_id", "customer_id", "product_name", "price", "quantity", "order_date", "region"])
    df_test.coalesce(1).write.mode("overwrite").csv("data/raw/orders.csv", header=True)
    
    # Run ETL
    run_etl()
    
    # Load result and validate
    result_df = spark.read.parquet("data/processed/orders_clean.parquet")
    assert result_df.filter(col("customer_id") == "CUST_123").count() == 1
    assert result_df.filter(col("price") == 100.0).count() == 1
    
    spark.stop()
