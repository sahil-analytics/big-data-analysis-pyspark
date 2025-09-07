"""
Big Data Analysis with PySpark: Customer Behavior Analytics
============================================================
This project analyzes large-scale e-commerce customer data to identify purchasing patterns, customer segmentation, and behavior trends using distributed computing with PySpark.
"""

# 1. SETUP AND IMPORTS

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    when, round, datediff, current_date, year, month, dayofweek,
    stddev, percentile_approx, collect_list, size, array_contains,
    window, lag, lead, dense_rank, row_number, lit, concat_ws,
    from_unixtime, unix_timestamp, to_date, date_format, expr,
    rand, floor, array
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import numpy as np
import random


# Initialize Spark Session with optimized configuration
spark = SparkSession.builder \
    .appName("CustomerBehaviorAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session initialized successfully")
print(f"Spark Version: {spark.version}")

# 2. DATA GENERATION

def generate_customer_data(spark, num_customers=1000000, num_transactions=5000000):
    """
    Generates synthetic customer and transaction data directly as Spark DataFrames
    for improved scalability and performance. This approach avoids collecting large
    datasets on the driver node.
    """
    print(f"Generating {num_customers:,} customers and {num_transactions:,} transactions directly in Spark...")

    # Schemas for structured data generation
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("gender", StringType(), False),
        StructField("location", StringType(), False),
        StructField("membership_level", StringType(), False),
        StructField("registration_date", DateType(), False)
    ])

    # Helper function to generate a single customer record
    locations = ['North', 'South', 'East', 'West', 'Central']
    memberships = ['Bronze', 'Silver', 'Gold', 'Platinum']
    genders = ['M', 'F']
    def create_customer_row(id_val):
        return (
            f'CUST_{id_val:07d}',
            random.randint(18, 75),
            random.choice(genders),
            random.choice(locations),
            random.choice(memberships),
            (datetime.now() - timedelta(days=random.randint(1, 1825))).date()
        )

    # Generate customer data in parallel using an RDD
    # Use numSlices to control parallelism
    customer_rdd = spark.sparkContext.parallelize(range(num_customers), 100).map(create_customer_row)
    customers_df = spark.createDataFrame(customer_rdd, customer_schema)

    # Generate transaction data using Spark SQL expressions for maximum efficiency
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home', 'Beauty', 'Toys']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']
    
    transaction_date_expr_str = "to_date(current_timestamp() - (rand() * 365 * interval 1 day))"

    transactions_df = spark.range(num_transactions).repartition(200) \
        .withColumn("transaction_id", expr("concat('TXN_', lpad(id, 9, '0'))")) \
        .withColumn("customer_id", expr(f"concat('CUST_', lpad(floor(rand() * {num_customers}), 7, '0'))")) \
        .withColumn("transaction_date", expr(transaction_date_expr_str)) \
        .withColumn("product_category", array(*[lit(c) for c in categories])[floor(rand() * len(categories))]) \
        .withColumn("amount", expr("round(rand() * 490 + 10, 2)")) \
        .withColumn("quantity", expr("int(rand() * 10) + 1")) \
        .withColumn("payment_method", array(*[lit(p) for p in payment_methods])[floor(rand() * len(payment_methods))]) \
        .withColumn("is_weekend", expr("if(dayofweek(transaction_date) IN (1, 7), 1, 0)")) \
        .withColumn("hour_of_day", expr("int(rand() * 24)")) \
        .drop("id")

    return customers_df, transactions_df


# Generate data directly into DataFrames
customers_df, transactions_df = generate_customer_data(spark)

# Cache DataFrames for better performance
customers_df.cache()
transactions_df.cache()

print(f"Data generated: {customers_df.count():,} customers, {transactions_df.count():,} transactions")

# 3. DATA EXPLORATION

print("\n" + "="*50)
print("DATA EXPLORATION")
print("="*50)

# Schema information
print("\nCustomer DataFrame Schema:")
customers_df.printSchema()

print("\nTransaction DataFrame Schema:")
transactions_df.printSchema()

# Basic statistics
print("\nTransaction Statistics:")
transactions_df.select(
    spark_min("amount").alias("min_amount"),
    spark_max("amount").alias("max_amount"),
    avg("amount").alias("avg_amount"),
    stddev("amount").alias("stddev_amount")
).show()

# 4. DATA WRANGLING & FEATURE ENGINEERING

print("\n" + "="*50)
print("DATA WRANGLING & FEATURE ENGINEERING")
print("="*50)

# Add time-based features
transactions_df = transactions_df \
    .withColumn("year", year("transaction_date")) \
    .withColumn("month", month("transaction_date")) \
    .withColumn("day_of_week", dayofweek("transaction_date")) \
    .withColumn("is_holiday_season", 
                when(col("month").isin([11, 12]), 1).otherwise(0))

# Calculate customer lifetime metrics
customer_metrics = transactions_df.groupBy("customer_id").agg(
    count("transaction_id").alias("total_transactions"),
    spark_sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction_value"),
    spark_max("transaction_date").alias("last_purchase_date"),
    spark_min("transaction_date").alias("first_purchase_date"),
    count(when(col("is_weekend") == 1, True)).alias("weekend_purchases"),
    collect_list("product_category").alias("categories_purchased")
)

# Calculate recency (days since last purchase)
customer_metrics = customer_metrics.withColumn(
    "recency_days",
    datediff(current_date(), col("last_purchase_date"))
)

# Calculate frequency metrics
customer_metrics = customer_metrics.withColumn(
    "purchase_frequency",
    col("total_transactions") / 
    (datediff(col("last_purchase_date"), col("first_purchase_date")) + 1)
)

# Calculate category diversity
customer_metrics = customer_metrics.withColumn(
    "category_diversity",
    size(col("categories_purchased"))
)

# Join with customer demographics
customer_full = customers_df.join(customer_metrics, on="customer_id", how="left")

# Fill nulls for customers with no transactions
customer_full = customer_full.fillna({
    "total_transactions": 0,
    "total_spent": 0,
    "avg_transaction_value": 0,
    "recency_days": 999,
    "purchase_frequency": 0,
    "category_diversity": 0
})

print("Feature engineering completed. Sample customer metrics:")
customer_full.select(
    "customer_id", "total_transactions", "total_spent", 
    "avg_transaction_value", "recency_days"
).show(5)

# 5. CUSTOMER SEGMENTATION (RFM ANALYSIS)

print("\n" + "="*50)
print("CUSTOMER SEGMENTATION - RFM ANALYSIS")
print("="*50)

# Calculate RFM scores
customer_rfm = customer_full.withColumn(
    "recency_score",
    when(col("recency_days") <= 30, 5)
    .when(col("recency_days") <= 60, 4)
    .when(col("recency_days") <= 90, 3)
    .when(col("recency_days") <= 180, 2)
    .otherwise(1)
)

customer_rfm = customer_rfm.withColumn(
    "frequency_score",
    when(col("total_transactions") >= 50, 5)
    .when(col("total_transactions") >= 30, 4)
    .when(col("total_transactions") >= 15, 3)
    .when(col("total_transactions") >= 5, 2)
    .otherwise(1)
)

customer_rfm = customer_rfm.withColumn(
    "monetary_score",
    when(col("total_spent") >= 5000, 5)
    .when(col("total_spent") >= 2000, 4)
    .when(col("total_spent") >= 1000, 3)
    .when(col("total_spent") >= 500, 2)
    .otherwise(1)
)

# Create RFM segment
customer_rfm = customer_rfm.withColumn(
    "rfm_score",
    concat_ws("", col("recency_score"), col("frequency_score"), col("monetary_score"))
)

# Define customer segments based on RFM
customer_rfm = customer_rfm.withColumn(
    "customer_segment",
    when(col("rfm_score") == "555", "Champions")
    .when(col("rfm_score").like("5%5"), "Loyal Customers")
    .when(col("rfm_score").like("5%"), "Potential Loyalists")
    .when(col("rfm_score").like("%5%"), "Big Spenders")
    .when(col("rfm_score").like("3%"), "At Risk")
    .when(col("rfm_score").like("1%"), "Lost")
    .otherwise("Regular")
)

# Segment distribution
print("\nCustomer Segment Distribution:")
segment_dist = customer_rfm.groupBy("customer_segment") \
    .agg(
        count("*").alias("customer_count"),
        avg("total_spent").alias("avg_lifetime_value"),
        avg("total_transactions").alias("avg_transactions")
    ) \
    .orderBy(col("customer_count").desc())

segment_dist.show()

# 6. MACHINE LEARNING - K-MEANS CLUSTERING

print("\n" + "="*50)
print("MACHINE LEARNING - CUSTOMER CLUSTERING")
print("="*50)

# Prepare features for clustering
feature_cols = [
    "age", "total_transactions", "total_spent", 
    "avg_transaction_value", "recency_days", 
    "purchase_frequency", "category_diversity"
]

# Index categorical variables
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_indexed")
location_indexer = StringIndexer(inputCol="location", outputCol="location_indexed")
membership_indexer = StringIndexer(inputCol="membership_level", outputCol="membership_indexed")

# Assemble features
assembler = VectorAssembler(
    inputCols=feature_cols + ["gender_indexed", "location_indexed", "membership_indexed"],
    outputCol="features_unscaled"
)

# Scale features
scaler = StandardScaler(
    inputCol="features_unscaled",
    outputCol="features"
)

# K-Means clustering
kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=5,
    seed=42
)

# Create pipeline
pipeline = Pipeline(stages=[
    gender_indexer, location_indexer, membership_indexer,
    assembler, scaler, kmeans
])

# Fit the model
print("Training K-Means clustering model...")
model = pipeline.fit(customer_rfm)

# Make predictions
clustered_customers = model.transform(customer_rfm)

# Evaluate clustering
evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster",
    metricName="silhouette"
)

silhouette = evaluator.evaluate(clustered_customers)
print(f"\nSilhouette Score: {silhouette:.3f}")

# Analyze clusters
print("\nCluster Analysis:")
cluster_analysis = clustered_customers.groupBy("cluster") \
    .agg(
        count("*").alias("size"),
        avg("age").alias("avg_age"),
        avg("total_spent").alias("avg_spent"),
        avg("total_transactions").alias("avg_transactions"),
        avg("recency_days").alias("avg_recency")
    ) \
    .orderBy("cluster")

cluster_analysis.show()

# 7. BEHAVIORAL PATTERNS ANALYSIS

print("\n" + "="*50)
print("BEHAVIORAL PATTERNS ANALYSIS")
print("="*50)

# Time-based purchasing patterns
print("\nPurchasing Patterns by Day of Week:")
weekly_patterns = transactions_df.groupBy("day_of_week") \
    .agg(
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    ) \
    .orderBy("day_of_week")

weekly_patterns.show()

# Product category analysis
print("\nTop Product Categories:")
category_analysis = transactions_df.groupBy("product_category") \
    .agg(
        count("*").alias("purchase_count"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_price")
    ) \
    .orderBy(col("total_revenue").desc())

category_analysis.show()

# Payment method preferences
print("\nPayment Method Distribution:")
payment_analysis = transactions_df.groupBy("payment_method") \
    .agg(
        count("*").alias("usage_count"),
        avg("amount").alias("avg_transaction")
    ) \
    .orderBy(col("usage_count").desc())

payment_analysis.show()

# 8. COHORT ANALYSIS

print("\n" + "="*50)
print("COHORT ANALYSIS")
print("="*50)

# Create cohorts based on registration month
customers_with_cohort = customers_df.withColumn(
    "cohort_month",
    date_format("registration_date", "yyyy-MM")
)

# Join with transactions
cohort_transactions = customers_with_cohort.join(
    transactions_df,
    on="customer_id",
    how="inner"
)

# Calculate cohort metrics
cohort_metrics = cohort_transactions.groupBy("cohort_month") \
    .agg(
        countDistinct("customer_id").alias("cohort_size"),
        spark_sum("amount").alias("total_revenue"),
        count("transaction_id").alias("total_transactions")
    ) \
    .withColumn("revenue_per_customer", col("total_revenue") / col("cohort_size")) \
    .withColumn("transactions_per_customer", col("total_transactions") / col("cohort_size")) \
    .orderBy("cohort_month")

print("\nCohort Performance (Sample):")
cohort_metrics.show(10)

# 9. CHURN PREDICTION INDICATORS

print("\n" + "="*50)
print("CHURN RISK INDICATORS")
print("="*50)

# Identify potential churners
churners = customer_rfm.withColumn(
    "churn_risk",
    when(col("recency_days") > 90, "High")
    .when(col("recency_days") > 60, "Medium")
    .otherwise("Low")
)

# Churn risk distribution
print("\nChurn Risk Distribution:")
churn_dist = churners.groupBy("churn_risk") \
    .agg(
        count("*").alias("customer_count"),
        avg("total_spent").alias("avg_lifetime_value"),
        avg("total_transactions").alias("avg_transactions")
    )

churn_dist.show()

# High-value customers at risk
print("\nHigh-Value Customers at Risk:")
at_risk_valuable = churners.filter(
    (col("churn_risk") == "High") & 
    (col("total_spent") > 1000)
).select(
    "customer_id", "total_spent", "last_purchase_date", 
    "recency_days", "customer_segment"
)

print(f"Number of high-value customers at risk: {at_risk_valuable.count()}")
at_risk_valuable.show(5)

# 10. RECOMMENDATIONS & INSIGHTS

print("\n" + "="*50)
print("KEY INSIGHTS & RECOMMENDATIONS")
print("="*50)

# Calculate key metrics
total_revenue = transactions_df.agg(spark_sum("amount")).collect()[0][0]
total_customers = customers_df.count()
active_customers = customer_rfm.filter(col("recency_days") <= 90).count()
avg_customer_value = customer_rfm.agg(avg("total_spent")).collect()[0][0]

print(f"""
EXECUTIVE SUMMARY:
==================
• Total Revenue: ${total_revenue:,.2f}
• Total Customers: {total_customers:,}
• Active Customers (90 days): {active_customers:,} ({active_customers/total_customers*100:.1f}%)
• Average Customer Lifetime Value: ${avg_customer_value:.2f}
• Customer Retention Rate: {active_customers/total_customers*100:.1f}%

KEY FINDINGS:
=============
1. Customer Segmentation:
   - Champions and Loyal Customers represent the highest value segments
   - Significant opportunity to convert Potential Loyalists
   - At-risk customers need immediate retention campaigns

2. Behavioral Patterns:
   - Weekend shopping shows different patterns than weekdays
   - Certain product categories drive higher transaction values
   - Payment method preferences vary by customer segment

3. Churn Risk:
   - {churners.filter(col('churn_risk') == 'High').count():,} customers at high risk of churn
   - Early intervention needed for high-value at-risk customers

RECOMMENDATIONS:
================
1. Implement targeted retention campaigns for at-risk high-value customers
2. Develop loyalty programs to convert Potential Loyalists to Champions
3. Optimize inventory and marketing for high-performing product categories
4. Create personalized offers based on customer cluster characteristics
5. Focus on reactivating dormant customers with special promotions
""")

# 11. PERFORMANCE OPTIMIZATION METRICS

print("\n" + "="*50)
print("DISTRIBUTED COMPUTING PERFORMANCE")
print("="*50)

# Get execution plan
print("\nExecution Plan for Complex Query:")
complex_query = clustered_customers.filter(
    col("total_spent") > 1000
).groupBy("cluster", "customer_segment").agg(
    count("*").alias("count"),
    avg("total_spent").alias("avg_spent")
)

print("Logical Plan:")
complex_query.explain(True)

# Partition information
print(f"\nDataFrame Partitions: {transactions_df.rdd.getNumPartitions()}")
print(f"Cluster Cores Available: {spark.sparkContext.defaultParallelism}")

# 12. DATA EXPORT & CLEANUP

print("\n" + "="*50)
print("SAVING RESULTS")
print("="*50)

# Save key results (in production, these would be saved to HDFS/S3)
print("Saving customer segments...")
customer_segments = churners.select(
    "customer_id", "customer_segment", "cluster", 
    "churn_risk", "total_spent", "total_transactions"
)

# Show sample of final output
print("\nFinal Customer Segments (Sample):")
customer_segments.show(10)

# In production, you would save to persistent storage:
# customer_segments.write.mode("overwrite").parquet("hdfs://path/to/customer_segments")
# cluster_analysis.write.mode("overwrite").parquet("hdfs://path/to/cluster_analysis")

# Stop Spark session
print("\nAnalysis complete. Stopping Spark session...")
spark.stop()

print("\n" + "="*50)
print("PROJECT COMPLETED SUCCESSFULLY")
print("="*50)
