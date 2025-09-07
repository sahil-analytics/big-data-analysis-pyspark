# spark_config.py
from pyspark.sql import SparkSession

def create_spark_session(app_name="CustomerAnalysis", memory="4g"):
    """Create and configure Spark session for local development"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", memory) \
        .config("spark.executor.memory", memory) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    return spark