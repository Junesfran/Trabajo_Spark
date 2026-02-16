from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .master("spark://172.31.16.159:7077") \
        .appName("Consulta B") \
        .getOrCreate()

order_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/order_items.csv")

orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/orders.csv")

products = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/products.csv")

order_items_enriched = order_items.withColumn(
    "importe_linea",
    F.col("quantity") * F.col("unit_price")
)


df = order_items_enriched \
    .join(orders, "order_id") \
    .join(products, "product_id")


df = df.withColumn(
    "year_month",
    F.date_format("order_date", "yyyy-MM")
)

resultado = df.groupBy("year_month", "category") \
    .agg(
        F.countDistinct("customer_id").alias("clientes_unicos"),
        F.countDistinct("order_id").alias("num_pedidos"),
        F.sum("importe_linea").alias("importe_total")
    ) \
    .withColumn(
        "ticket_media",
        F.col("importe_total") / F.col("num_pedidos")
    ) \
    .orderBy("year_month", "category")

resultado.show()

resultado.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://hugito-quesadilla/resultado/consultaB_csv/")

resultado.write \
    .mode("overwrite") \
    .parquet("s3a://hugito-quesadilla/resultado/consultaB_parquet/")
