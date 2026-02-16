from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .master("spark://172.31.16.159:7077") \
        .appName("Consulta A") \
        .getOrCreate()


orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/orders.csv")


order_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/order_items.csv")


order_items_enriched = order_items.withColumn(
    "importe_linea",
    F.col("quantity") * F.col("unit_price")
)

df = order_items_enriched.join(
    orders.select("order_id", "order_date"),
    "order_id"
)

ventas_diarias = df.groupBy("order_date", "product_id") \
    .agg(
        F.sum("quantity").alias("unidades"),
        F.sum("importe_linea").alias("importe_total")
    )

window = Window.partitionBy("order_date").orderBy(F.col("importe_total").desc())

ventas_top = ventas_diarias.withColumn(
    "ranking",
    F.row_number().over(window)
).filter(F.col("ranking") <= 10)

resultado = ventas_top.select(
    F.col("order_date").alias("fecha"),
    "product_id",
    "unidades",
    "importe_total",
    "ranking"
).orderBy("fecha", "ranking")

resultado.show(20)

resultado.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://hugito-quesadilla/resultado/consultaA_csv/")

resultado.write \
    .mode("overwrite") \
    .parquet("s3a://hugito-quesadilla/resultado/consultaA_parquet/")