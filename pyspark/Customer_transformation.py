from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("SAS_Migration").getOrCreate()

# Read datasets
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)

# Filter orders
filtered_orders = orders.filter(col("amount") > 100)

# Aggregate orders
aggregated_orders = (
    filtered_orders
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_amount"))
)

# Join with customers
final_df = aggregated_orders.join(customers, "customer_id", "inner")

# Output
final_df.show()
