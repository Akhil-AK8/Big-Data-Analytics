# Big-Data-Analytics
# Step 1: Install PySpark
!pip install -q pyspark

# Step 2: Initialize Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, avg, count
import time

spark = SparkSession.builder \
    .appName("BigDataAnalyticsDemo") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("SparkSession created")

# Step 3: Generate Synthetic Big Data (~10 million rows)
start_time = time.time()

num_rows = 10_000_000

df = spark.range(0, num_rows) \
    .withColumn("user_id", (col("id") % 100000).cast("integer")) \
    .withColumn("age", (rand() * 60 + 18).cast("integer")) \
    .withColumn("gender", when(rand() > 0.5, "Male").otherwise("Female")) \
    .withColumn("purchase_amount", (rand() * 500).cast("float"))

print(f"DataFrame with {num_rows:,} rows created in {time.time() - start_time:.2f} seconds")

# Step 4: Big Data Transformation Example
start_time = time.time()

# Filter adults
adults_df = df.filter(col("age") >= 18)

# Group by gender and compute average purchase
result_df = adults_df.groupBy("gender").agg(
    avg("purchase_amount").alias("avg_purchase"),
    count("*").alias("total_users")
)

result_df.show()

print(f"Aggregation completed in {time.time() - start_time:.2f} seconds")

# Step 5: Optional - Save to Parquet (simulate big data storage)
output_path = "/tmp/big_data_output.parquet"
result_df.write.mode("overwrite").parquet(output_path)
print(f"Result saved to {output_path}")
