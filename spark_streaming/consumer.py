import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc, to_timestamp, window
from pyspark.sql.types import StructType, StringType

# Set Hadoop and Spark paths for Windows
os.environ["HADOOP_HOME"] = r"E:\BigDataTools\hadoop-3.2.1"
os.environ["SPARK_HOME"] = r"E:\BigDataTools\spark-3.3.2-bin-hadoop3"
os.environ["PATH"] += os.pathsep + r"E:\BigDataTools\hadoop-3.2.1\bin"

# Define Kafka message schema
crime_schema = StructType() \
    .add("ID", StringType()) \
    .add("Case Number", StringType()) \
    .add("Date", StringType()) \
    .add("Primary Type", StringType()) \
    .add("Description", StringType()) \
    .add("Location Description", StringType()) \
    .add("Arrest", StringType()) \
    .add("Domestic", StringType()) \
    .add("District", StringType()) \
    .add("Year", StringType())

# Spark Session with Delta + Kafka
spark = SparkSession.builder \
    .appName("CrimeKafkaSparkConsumerDelta") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark Version:", spark.version)
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crime_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_json.select(from_json(col("json_str"), crime_schema).alias("crime")).select("crime.*")

# Clean column names
def clean_column_name(name):
    return re.sub(r'[ ,;{}()\n\t=]', '_', name)

df_cleaned = df_parsed.select([col(c).alias(clean_column_name(c)) for c in df_parsed.columns])

# Use cleaned DataFrame for everything
df_filtered = df_cleaned.filter(col("Primary_Type").isNotNull())

# ===============================
# Query 1: Crime type counts
# ===============================
crime_type_counts = df_filtered.groupBy("Primary_Type") \
    .agg(count("*").alias("crime_count")) \
    .orderBy(desc("crime_count"))

query1 = crime_type_counts.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", "./sink/checkpoint/crime_type_counts") \
    .option("path", "./sink/delta/crime_type_counts") \
    .start()

# ===============================
# Query 2: Arrest stats
# ===============================
arrest_stats = df_filtered.filter(col("Arrest") == "true") \
    .groupBy("Primary_Type") \
    .agg(count("*").alias("arrest_count")) \
    .orderBy(desc("arrest_count"))

query2 = arrest_stats.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", "./sink/checkpoint/arrest_stats") \
    .option("path", "./sink/delta/arrest_stats") \
    .start()

# ===============================
# Query 3: Time-windowed crime trends
# ===============================
df_ts = df_filtered.withColumn("timestamp", to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))

trending_crimes = df_ts \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("Primary_Type")
    ).agg(count("*").alias("count"))

query3 = trending_crimes.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "./sink/checkpoint/trending_crimes") \
    .option("path", "./sink/delta/trending_crimes") \
    .start()

# ===============================
# Query 4: Raw timestamped table
# ===============================
ts_table = df_ts.select(
    "timestamp", "Primary_Type", "Location_Description",
    "Arrest", "Domestic", "District", "Year"
)

query4 = ts_table.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "./sink/checkpoint/ts_table") \
    .option("path", "./sink/delta/ts_table") \
    .start()

# Await all
spark.streams.awaitAnyTermination()