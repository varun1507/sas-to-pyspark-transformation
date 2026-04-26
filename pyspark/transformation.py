from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("SAS_Migration").getOrCreate()

# Read data
df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

# Transformation
filtered_df = df.filter(col("amount") > 100)

result = (
    filtered_df
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_amount"))
)

# Output
result.show()
