import os
import sys
# To musi być ZANIM załaduje się Spark
os.environ["PYTHONIOENCODING"] = "utf-8"

import findspark
# findspark.init()
import shutil
import glob
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
import pyspark

# KONFIGURACJA - Unikalny folder dla każdego uruchomienia
run_id = str(uuid.uuid4())[:8]
CHECKPOINT_DIR = f"checkpoints/wiki_batch_{run_id}"

OUTPUT_RAW = "output_raw_edits"
OUTPUT_STATS = "output_stats_history"
TEMP_DIR = "temp_spark_write"

def clean_dir(path):
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except Exception:
            pass # Ignorujemy błędy usuwania na Windowsie

clean_dir(CHECKPOINT_DIR)
clean_dir(TEMP_DIR)
os.makedirs(OUTPUT_RAW, exist_ok=True)
os.makedirs(OUTPUT_STATS, exist_ok=True)

json_schema = StructType([
    StructField("time", StringType(), True),
    StructField("user", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("title", StringType(), True),
    StructField("length_diff", IntegerType(), True),
    StructField("comment", StringType(), True),
    StructField("revision_id", IntegerType(), True) # <-- DODANO
])

# SESJA SPARK - Dodano więcej pamięci dla stabilności na Windowsie
# DYNAMICZNE DOPASOWANIE WERSJI
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"

print(f"-> Wykryto PySpark w wersji {spark_version}. Ładowanie pakietu: {kafka_package}")

# SESJA SPARK - Stabilna wersja 3.5.1
session = SparkSession.builder \
    .master("local[*]") \
    .appName("WikipediaBatchProcessor") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

session.sparkContext.setLogLevel("WARN")

def save_custom_csv(df, target_folder, prefix):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{prefix}_{timestamp}.csv"
    try:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(TEMP_DIR)
        files = glob.glob(f"{TEMP_DIR}/*.csv")
        if files:
            source_file = files[0]
            destination = os.path.join(target_folder, filename)
            shutil.move(source_file, destination)
            print(f"  -> Pomyślnie wygenerowano plik: {filename}")
    except Exception as e:
        print(f"⚠ Błąd podczas zapisu CSV: {e}")
    finally:
        clean_dir(TEMP_DIR)

def process_batch(df, epoch_id):
    try:
        # Usunięto df.persist(), żeby nie zapychać RAMu na lokalnym Windowsie
        row_count = df.count()

        if row_count == 0:
            print(f"--- Batch {epoch_id}: Oczekiwanie na pełne okno czasu (brak konfliktów) ---")
            return

        print(f"\n{'=' * 20} BATCH ID: {epoch_id} | 🚨 WYKRYTO KONFLIKTY: {row_count} {'=' * 20}")

        alert_df = df.select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            col("title"),
            col("total_edits"),
            col("unique_users"),
            col("combatants"),
            col("latest_comment"),
            col("latest_revision")  # <-- DODANO
        ).orderBy(col("total_edits").desc())

        print("  Zapis wyników w tle...")
        alert_df_with_meta = alert_df.withColumn("batch_id", lit(epoch_id))
        save_custom_csv(alert_df_with_meta, OUTPUT_STATS, "edit_wars")

    except Exception as e:
        print(f"\n!!! BŁĄD W PYTHONIE (Batch {epoch_id}): {e}")

def start_app():
    print("Oczekiwanie na dane z Kafki...")

    raw_stream = (
        session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "wiki-edits")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), json_schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("editor_type", when(col("is_bot") == True, "BOT").otherwise("HUMAN"))
        .filter(~col("title").startswith("Category:"))
        .filter(~col("title").startswith("Talk:"))
    )

    edit_wars_stream = (
        parsed_stream
        .withWatermark("timestamp", "5 minutes")
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("title")
        )
        .agg(
            count("user").alias("total_edits"),
            approx_count_distinct("user").alias("unique_users"),
            concat_ws(", ", collect_set("user")).alias("combatants"),
            last("comment").alias("latest_comment"),
            last("revision_id").alias("latest_revision")  # <-- DODANO
        )
        # Próg alertu: min. 4 edycje OD MINIMUM 2 RÓŻNYCH UŻYTKOWNIKÓW (żeby wykluczyć pojedyncze boty)
        .filter((col("total_edits") >= 4) & (col("unique_users") > 1))
    )

    query = (
        edit_wars_stream.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    query.awaitTermination()

# ODKOMENTOWANE! Bez tego skrypt w ogóle nie ruszy z konsoli.
if __name__ == "__main__":
    try:
        start_app()
    except KeyboardInterrupt:
        print("Zatrzymywanie...")