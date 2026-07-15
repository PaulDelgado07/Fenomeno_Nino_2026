#Carga los datos SST procesados de HDFS en PostgreSQL.

from pyspark.sql import SparkSession


HDFS_INPUT_PATH = "hdfs://namenode:9000/el_nino/streaming"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/el_nino"
POSTGRES_TABLE = "sst_procesada"
POSTGRES_PROPERTIES = {
    "user": "el_nino",
    "password": "el_nino_2026",
    "driver": "org.postgresql.Driver",
}


spark = (
    SparkSession.builder
    .appName("LoadSSTToPostgres")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Leyendo datos procesados desde HDFS...")
sst_df = spark.read.parquet(HDFS_INPUT_PATH)

row_count = sst_df.count()
if row_count == 0:
    raise RuntimeError("No hay registros Parquet para cargar en PostgreSQL.")

print(f"Cargando {row_count} registros en PostgreSQL...")
(
    sst_df.write
    .mode("overwrite")
    .jdbc(POSTGRES_URL, POSTGRES_TABLE, properties=POSTGRES_PROPERTIES)
)

print(f"Carga terminada: tabla {POSTGRES_TABLE} actualizada con {row_count} registros.")
spark.stop()
