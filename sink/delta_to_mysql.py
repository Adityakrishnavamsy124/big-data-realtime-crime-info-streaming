import os
import sys
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pathlib import Path
import time

# ✅ 1. Set environment variables (BEFORE SparkSession)
java_home = ""
hadoop_home = r"E:\BigDataTools\hadoop-3.2.1"
spark_home = r"E:\BigDataTools\spark-3.3.2-bin-hadoop3"

os.environ["JAVA_HOME"] = java_home
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["SPARK_HOME"] = spark_home

# Prepend Java and Hadoop to PATH
os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + \
                     os.path.join(hadoop_home, "bin") + os.pathsep + \
                     os.environ["PATH"]

# ✅ 2. Initialize Spark
def init_spark():
    builder = SparkSession.builder \
        .appName("DeltaToMySQL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "io.delta:delta-core_2.12:1.2.1")

    return configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ 3. Write to MySQL
def write_to_mysql(df, table_name, url, props):
    try:
        df.write.jdbc(
            url=url,
            table=table_name,
            mode="overwrite",
            properties=props
        )
        print(f"✅ Overwritten MySQL table: {table_name}")
    except Exception as e:
        print(f"❌ Failed to write {table_name}: {e}")

# ✅ 4. Loop through Delta folders
def process_all_delta_tables(base_path, spark, mysql_url, mysql_props):
    for folder in Path(base_path).iterdir():
        if folder.is_dir():
            table_name = folder.name.replace("-", "_")
            try:
                df = spark.read.format("delta").load(str(folder))
                write_to_mysql(df, table_name, mysql_url, mysql_props)
            except Exception as e:
                print(f"❌ Failed to process {folder.name}: {e}")

# ✅ 5. Run main sync loop
if __name__ == "__main__":
    print("🔧 Starting Spark + Delta job...")
    spark = init_spark()
    base_path = "sink/delta"

    mysql_url = "jdbc:mysql://localhost:3306/grafana_data"
    mysql_props = {
        "user": "root",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    while True:
        print("🔄 Syncing Delta tables to MySQL...")
        process_all_delta_tables(base_path, spark, mysql_url, mysql_props)
        print("⏳ Waiting 30 seconds...")
        time.sleep(30)
