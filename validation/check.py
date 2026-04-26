from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Validation").getOrCreate()

df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

# Expected result (manual logic)
expected = (
    df.filter("amount > 100")
      .groupBy("customer_id")
      .sum("amount")
)

print("Validation Completed - Row Count:", expected.count())
