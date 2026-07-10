from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "sst-noaa"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/el_nino/streaming"
HDFS_CHECKPOINT_PATH = "hdfs://namenode:9000/el_nino/checkpoints"
CONSOLE_CHECKPOINT_PATH = "hdfs://namenode:9000/el_nino/checkpoints_console"

spark = (
    SparkSession.builder
    .appName("KafkaStreamingSST")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("variable", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField(
        "location",
        StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ]),
        True
    )
])

print("==============================")
print("SPARK ESCUCHANDO KAFKA")
print("==============================")


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


json_df = (
    df.selectExpr("CAST(value AS STRING) AS mensaje")
    .select(from_json(col("mensaje"), schema).alias("data"))
)

datos = json_df.select(
    col("data.timestamp"),
    col("data.source"),
    col("data.variable"),
    col("data.value"),
    col("data.unit"),
    col("data.location.lat").alias("lat"),
    col("data.location.lon").alias("lon")
)

datos_procesados = (
    datos.withColumn(
        "estado",
        when(col("value") < 26, "Fría")
        .when(col("value") < 29, "Normal")
        .otherwise("Posible El Niño")
    )
)

# Guarda los mensajes procesados como Parquet en HDFS.
parquet_query = (
    datos_procesados.writeStream
    .format("parquet")
    .option("path", HDFS_OUTPUT_PATH)
    .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# Muestra los mismos mensajes en pantalla para comprobar el flujo en tiempo real.
console_query = (
    datos_procesados.writeStream
    .format("console")
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", CONSOLE_CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
