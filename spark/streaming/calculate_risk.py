"""Calcula en streaming el riesgo de inundación por zona de Guayaquil."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, from_json, lit, max, to_date, to_timestamp, when, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA_TOPICS = "precip-gpm,mareas-inocar,nivel-embalse-celec"
RAW_PATH = "hdfs://namenode:9000/el_nino/raw/hidrometeorologia"
STAGING_PATH = "hdfs://namenode:9000/el_nino/staging/hidrometeorologia"
PROCESSED_PATH = "hdfs://namenode:9000/el_nino/processed/riesgo_zonas"
RAW_CHECKPOINT = "hdfs://namenode:9000/el_nino/checkpoints_raw_hidro"
STAGING_CHECKPOINT = "hdfs://namenode:9000/el_nino/checkpoints_staging_hidro"
RISK_CHECKPOINT = "hdfs://namenode:9000/el_nino/checkpoints_riesgo"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/el_nino"
POSTGRES_PROPERTIES = {
    "user": "el_nino",
    "password": "el_nino_2026",
    "driver": "org.postgresql.Driver",
}


schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("source", StringType(), False),
    StructField("variable", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), False),
    StructField("zone", StringType(), False),
    StructField("elevation_m", DoubleType(), False),
    StructField("base_vulnerability", DoubleType(), False),
    StructField("location", StructType([
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
    ]), False),
])

sngr_schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("source", StringType(), False),
    StructField("province", StringType(), False),
    StructField("canton", StringType(), False),
    StructField("zone", StringType(), False),
    StructField("alert_level", StringType(), False),
    StructField("description", StringType(), False),
    StructField("location", StructType([
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
    ]), False),
])



spark = (
    SparkSession.builder
    .appName("FloodRiskGuayaquil")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", KAFKA_TOPICS)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Zona raw: el evento Kafka original, sin transformar.
raw_query = (
    kafka_df.selectExpr("topic", "partition", "offset", "timestamp AS kafka_timestamp", "CAST(value AS STRING) AS payload")
    .writeStream
    .format("parquet")
    .option("path", RAW_PATH)
    .option("checkpointLocation", RAW_CHECKPOINT)
    .outputMode("append")
    .start()
)

events = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("event"))
    .select("event.*")
    .withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("event_date", to_date("event_time"))
    .withColumn("lat", col("location.lat"))
    .withColumn("lon", col("location.lon"))
    .drop("location")
)

# Zona staging: datos limpios y normalizados, particionados por fecha y fuente.
staging_query = (
    events.writeStream
    .format("parquet")
    .option("path", STAGING_PATH)
    .option("checkpointLocation", STAGING_CHECKPOINT)
    .partitionBy("event_date", "source")
    .outputMode("append")
    .start()
)

aggregated = (
    events.withWatermark("event_time", "30 seconds")
    .groupBy(
        window("event_time", "30 seconds", "5 seconds"),
        "zone", "lat", "lon", "elevation_m", "base_vulnerability",
    )
    .agg(
        max(when(col("variable") == "precipitation_mm_h", col("value"))).alias("precipitation_mm_h"),
        max(when(col("variable") == "tide_m", col("value"))).alias("tide_m"),
        max(when(col("variable") == "reservoir_pct", col("value"))).alias("reservoir_pct"),
    )
)

risk = (
    aggregated
    .withColumn("precipitation_mm_h", coalesce(col("precipitation_mm_h"), lit(0.0)))
    .withColumn("tide_m", coalesce(col("tide_m"), lit(0.0)))
    .withColumn("reservoir_pct", coalesce(col("reservoir_pct"), lit(0.0)))
    .withColumn("precipitation_score", when(col("precipitation_mm_h") >= 45, 35).when(col("precipitation_mm_h") >= 25, 22).otherwise(8))
    .withColumn("tide_score", when(col("tide_m") >= 3.2, 25).when(col("tide_m") >= 2.5, 15).otherwise(5))
    .withColumn("reservoir_score", when(col("reservoir_pct") >= 90, 15).when(col("reservoir_pct") >= 80, 8).otherwise(3))
    .withColumn("vulnerability_score", col("base_vulnerability") * 20)
    .withColumn("combined_bonus", when((col("precipitation_mm_h") >= 25) & (col("tide_m") >= 2.5), 10).otherwise(0))
    .withColumn("risk_index", col("precipitation_score") + col("tide_score") + col("reservoir_score") + col("vulnerability_score") + col("combined_bonus"))
    .withColumn("risk_level", when(col("risk_index") >= 70, "Crítico").when(col("risk_index") >= 50, "Alto").when(col("risk_index") >= 30, "Medio").otherwise("Bajo"))
    .select(
        col("window.end").alias("calculated_at"), "zone", "lat", "lon", "elevation_m",
        "precipitation_mm_h", "tide_m", "reservoir_pct", "risk_index", "risk_level",
    )
)


def save_risk(batch_df, batch_id):
    """Guarda el cálculo en HDFS y conserva en PostgreSQL el estado vigente por zona."""
    if batch_df.isEmpty():
        return

    batch_df.persist()
    batch_df.write.mode("append").parquet(PROCESSED_PATH)
    batch_df.write.mode("overwrite").jdbc(POSTGRES_URL, "riesgo_zonas", properties=POSTGRES_PROPERTIES)
    print(f"Lote de riesgo {batch_id} almacenado para {batch_df.count()} zonas.")
    batch_df.unpersist()


risk_query = (
    risk.writeStream
    .foreachBatch(save_risk)
    .option("checkpointLocation", RISK_CHECKPOINT)
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)

# --- FLUJO DE ALERTAS SNGR ---
sngr_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "alertas-sngr")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# HDFS Raw para alertas SNGR
sngr_raw_query = (
    sngr_df.selectExpr("topic", "partition", "offset", "timestamp AS kafka_timestamp", "CAST(value AS STRING) AS payload")
    .writeStream
    .format("parquet")
    .option("path", "hdfs://namenode:9000/el_nino/raw/alertas_sngr")
    .option("checkpointLocation", "hdfs://namenode:9000/el_nino/checkpoints_raw_sngr")
    .outputMode("append")
    .start()
)

sngr_events = (
    sngr_df.select(from_json(col("value").cast("string"), sngr_schema).alias("event"))
    .select("event.*")
    .withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("event_date", to_date("event_time"))
    .withColumn("lat", col("location.lat"))
    .withColumn("lon", col("location.lon"))
    .drop("location")
)

# HDFS Staging para alertas SNGR
sngr_staging_query = (
    sngr_events.writeStream
    .format("parquet")
    .option("path", "hdfs://namenode:9000/el_nino/staging/alertas_sngr")
    .option("checkpointLocation", "hdfs://namenode:9000/el_nino/checkpoints_staging_sngr")
    .partitionBy("event_date", "alert_level")
    .outputMode("append")
    .start()
)

def save_sngr(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # Escribir a Postgres alertas_sngr
    batch_df.persist()
    batch_df.write.mode("append").jdbc(POSTGRES_URL, "alertas_sngr", properties=POSTGRES_PROPERTIES)
    print(f"Lote de alertas SNGR {batch_id} almacenado para {batch_df.count()} alertas.")
    batch_df.unpersist()

# Postgres Sink para alertas SNGR
sngr_postgres_query = (
    sngr_events.writeStream
    .foreachBatch(save_sngr)
    .option("checkpointLocation", "hdfs://namenode:9000/el_nino/checkpoints_postgres_sngr")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()

