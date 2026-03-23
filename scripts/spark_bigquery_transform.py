from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

PROJECT_ID = "energy-dss-1773915785"
SOURCE_DATASET = "energy_dss"
OUTPUT_DATASET = "energy_dss_spark"

SOURCE_TABLE = f"{PROJECT_ID}.{SOURCE_DATASET}.energy_load_clean"
OUTPUT_TABLE = f"{PROJECT_ID}.{OUTPUT_DATASET}.spark_energy_metrics"

spark = (
    SparkSession.builder
    .appName("Energy DSS Spark Transformation")
    .config(
        "spark.jars.packages",
        ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
            "javax.inject:javax.inject:1"
        ])
    )
    .getOrCreate()
)

print(f"Reading from BigQuery table: {SOURCE_TABLE}")

df = (
    spark.read.format("bigquery")
    .option("table", SOURCE_TABLE)
    .load()
)

print("Input schema:")
df.printSchema()
print(f"Input row count: {df.count()}")

result = (
    df.groupBy("country_code", "bidding_zone")
    .agg(
        avg("load_mw").alias("avg_load"),
        max("load_mw").alias("max_load"),
        min("load_mw").alias("min_load")
    )
)

print("Result schema:")
result.printSchema()
print(f"Result row count: {result.count()}")

print(f"Writing to BigQuery table: {OUTPUT_TABLE}")

(
    result.write.format("bigquery")
    .option("table", OUTPUT_TABLE)
    .option("writeMethod", "direct")
    .mode("overwrite")
    .save()
)

print("Spark transformation complete")

spark.stop()
