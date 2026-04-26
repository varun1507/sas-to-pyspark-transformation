from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SAS_Migration").getOrCreate()

df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

result = (
    df.filter("amount > 100")
      .groupBy("customer_id")
      .sum("amount")
)

result.show()
