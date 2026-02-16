from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://172.31.16.159:7077") \
    .appName("Consulta A") \
    .getOrCreate()

df = spark.range(5)
df.show()
df.explain()