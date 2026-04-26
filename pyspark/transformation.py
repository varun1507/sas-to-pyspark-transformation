from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark
spark = SparkSession.builder.appName("SAS_Migration").getOrCreate()

# -------------------------------
# Step 1: Read Input Data
# -------------------------------
orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)

# -------------------------------
# Step 2: Filter Data
# -------------------------------
filtered_orders = orders.filter(col("amount") > 100)

# -------------------------------
# Step 3: Aggregate Data
# -------------------------------
aggregated_orders = (
    filtered_orders
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_amount"))
)

# -------------------------------
# Step 4: Join with Customer Data
# -------------------------------
final_df = aggregated_orders.join(customers, "customer_id", "inner")

# -------------------------------
# Step 5: Output Result
# -------------------------------
final_df.show()
