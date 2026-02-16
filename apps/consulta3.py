from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .master("spark://172.31.16.159:7077") \
        .appName("Consulta C") \
        .getOrCreate()

order_items = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/order_items.csv")

orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://hugito-quesadilla/comercio360/JuanNestorFranco/raw/orders.csv")

order_items_enriched = order_items.withColumn(
    "importe_linea",
    F.col("quantity") * F.col("unit_price")
)

df = order_items_enriched.join(
    orders.select("order_id", "store_id", "order_date"),
    "order_id"
)

ventas_diarias = df.groupBy("store_id", "order_date") \
    .agg(
        F.sum("importe_linea").alias("ventas_dia")
    )

ventas_diarias = ventas_diarias.withColumn(
    "order_ts",
    F.col("order_date").cast("timestamp")
)

window_30d = Window.partitionBy("store_id") \
    .orderBy(F.col("order_ts").cast("long")) \
    .rangeBetween(-2592000, 0)

ventas_stats = ventas_diarias.withColumn(
    "media_30d",
    F.avg("ventas_dia").over(window_30d)
).withColumn(
    "desviacion_30d",
    F.stddev("ventas_dia").over(window_30d)
)

resultado = ventas_stats.withColumn(
    "is_outlier",
    F.when(
        F.col("ventas_dia") > 
        F.col("media_30d") + 2 * F.col("desviacion_30d"),
        True
    ).otherwise(False)
)

resultado_final = resultado.select(
    "store_id",
    "order_date",
    "ventas_dia",
    "media_30d",
    "desviacion_30d",
    "is_outlier"
).orderBy("store_id", "order_date")

resultado_final.show()

resultado.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3a://hugito-quesadilla/resultado/consultaC_csv/")

resultado.write \
    .mode("overwrite") \
    .parquet("s3a://hugito-quesadilla/resultado/consultaC_parquet/")
	