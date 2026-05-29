import os
import sys
import glob
import shutil
import uuid
from datetime import datetime

os.environ["PYTHONIOENCODING"] = "utf-8"

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, sum as spark_sum, last, approx_count_distinct,
    concat_ws, collect_set, window, when, lower, to_timestamp, from_json,
    split,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    IntegerType, DoubleType,
)

# ─────────────────────────────────────────────
#  KONFIGURACJA
# ─────────────────────────────────────────────
run_id          = str(uuid.uuid4())[:8]
CHECKPOINT_BASE = f"checkpoints/wiki_{run_id}"
OUTPUT_WARS     = "output_stats_history"
OUTPUT_EDITORS  = "output_super_editors"
OUTPUT_TOPICS   = "output_topic_trends"
OUTPUT_MAP      = "output_live_map"
TEMP_DIR        = "temp_spark_write"

for d in [OUTPUT_WARS, OUTPUT_EDITORS, OUTPUT_TOPICS, OUTPUT_MAP]:
    os.makedirs(d, exist_ok=True)

def clean_dir(path: str):
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except Exception:
            pass

clean_dir(TEMP_DIR)
clean_dir(CHECKPOINT_BASE)

# ─────────────────────────────────────────────
#  SCHEMAT JSON
# ─────────────────────────────────────────────
json_schema = StructType([
    StructField("time",          StringType(),  True),
    StructField("user",          StringType(),  True),
    StructField("is_bot",        BooleanType(), True),
    StructField("is_suspicious", BooleanType(), True),
    StructField("title",         StringType(),  True),
    StructField("wiki_lang",     StringType(),  True),
    StructField("length_diff",   IntegerType(), True),
    StructField("comment",       StringType(),  True),
    StructField("revision_id",   IntegerType(), True),
    StructField("is_minor",      BooleanType(), True),
    StructField("lat",           DoubleType(),  True),
    StructField("lon",           DoubleType(),  True),
])

# ─────────────────────────────────────────────
#  SPARK SESSION — dynamiczna wersja Kafka
# ─────────────────────────────────────────────
spark_ver     = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_ver}"
print(f"→ PySpark {spark_ver} | pakiet: {kafka_package}")

session = (
    SparkSession.builder
    .master("local[*]")
    .appName("WikiEditWarsV2")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.jars.packages", kafka_package)
    .getOrCreate()
)
session.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
#  NARZĘDZIA ZAPISU CSV
# ─────────────────────────────────────────────
def _write_csv(df, target_dir: str, prefix: str):
    ts  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp = f"{TEMP_DIR}_{prefix}"
    dest = os.path.join(target_dir, f"{prefix}_{ts}.csv")
    clean_dir(tmp)
    try:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
        files = glob.glob(f"{tmp}/*.csv")
        if files:
            shutil.move(files[0], dest)
            print(f"  → {dest}")
    except Exception as exc:
        print(f"  ⚠ Błąd zapisu CSV [{prefix}]: {exc}")
    finally:
        clean_dir(tmp)


# ─────────────────────────────────────────────
#  CALLBACKS FOREACHBATCH
# ─────────────────────────────────────────────

def process_edit_wars(df, epoch_id):
    try:
        n = df.count()
        if n == 0:
            print(f"--- Batch {epoch_id}: brak wojen edycyjnych ---")
            return
        print(f"\n{'='*20} BATCH {epoch_id} | ⚔ KONFLIKTY: {n} {'='*20}")
        result = df.select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "title", "total_edits", "unique_users",
            "combatants", "latest_comment", "latest_revision",
            "reverts_count",
        ).orderBy(col("total_edits").desc())
        _write_csv(result.withColumn("batch_id", lit(epoch_id)), OUTPUT_WARS, "edit_wars")
    except Exception as exc:
        print(f"!!! BŁĄD process_edit_wars (batch {epoch_id}): {exc}")


def process_super_editors(df, epoch_id):
    try:
        if df.count() == 0:
            return
        result = df.select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "user", "edit_count", "is_bot_flag",
            "is_suspicious_flag", "avg_length_diff", "langs_edited",
        ).orderBy(col("edit_count").desc()).limit(50)
        _write_csv(result.withColumn("batch_id", lit(epoch_id)), OUTPUT_EDITORS, "super_editors")
    except Exception as exc:
        print(f"!!! BŁĄD process_super_editors (batch {epoch_id}): {exc}")


def process_topic_trends(df, epoch_id):
    try:
        if df.count() == 0:
            return
        result = df.select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "topic", "article_count", "total_edits", "sample_titles",
        ).orderBy(col("total_edits").desc()).limit(20)
        _write_csv(result.withColumn("batch_id", lit(epoch_id)), OUTPUT_TOPICS, "topic_trends")
    except Exception as exc:
        print(f"!!! BŁĄD process_topic_trends (batch {epoch_id}): {exc}")


def update_live_map(df, epoch_id):
    """
    Zapisuje punkty mapy bezpośrednio przez pandas (nie Spark CSV writer).
    Akumuluje dane z poprzednich batchy (max 500 punktów) żeby mapa
    nie była pusta gdy mało IP-ów przyszło w jednym oknie.
    """
    import pandas as pd

    map_path = os.path.join(OUTPUT_MAP, "live_map.csv")
    try:
        total_in_batch = df.count()
        print(f"  🗺 Batch {epoch_id}: {total_in_batch} eventów w strumieniu map")

        # Przekształcamy bieżący batch
        new_df = df.select("lat", "lon", "wiki_lang").toPandas()
        new_df.dropna(subset=["lat", "lon"], inplace=True)
        print(f"  🗺 Z lat/lon: {len(new_df)} punktów")

        if new_df.empty:
            print(f"  🗺 Brak punktów z geo w tym batchu — pomijam zapis")
            return

        # Akumulacja: dołączamy do istniejącego pliku (max 500 wierszy)
        if os.path.exists(map_path):
            try:
                old_df = pd.read_csv(map_path)
                combined = pd.concat([old_df, new_df], ignore_index=True).tail(500)
            except Exception:
                combined = new_df
        else:
            combined = new_df

        combined.to_csv(map_path, index=False)
        print(f"  🗺 ✓ Zapisano {len(combined)} punktów → {map_path}")

    except Exception as exc:
        print(f"  ⚠ Błąd update_live_map (batch {epoch_id}): {exc}")
        import traceback
        traceback.print_exc()


# ─────────────────────────────────────────────
#  GŁÓWNA APLIKACJA
# ─────────────────────────────────────────────
def start_app():
    print("Oczekiwanie na dane z Kafki …")

    raw = (
        session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "wiki-edits")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), json_schema).alias("d"))
        .select("d.*")
        .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd %H:%M:%S"))
        .withColumn("editor_type",
                    when(col("is_suspicious"), "SUSPECTED_BOT")
                    .when(col("is_bot"), "BOT")
                    .otherwise("HUMAN"))
        .filter(~col("title").startswith("Category:"))
        .filter(~col("title").startswith("Talk:"))
    )

    # 1. WOJNY EDYCYJNE
    wars_agg = (
        parsed
        .withWatermark("timestamp", "5 minutes")
        .withColumn("is_revert",
                    when(lower(col("comment")).rlike(
                        r"revert|undid|undo|wycofano|przywrócono|revertido"
                    ), 1).otherwise(0))
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("title"))
        .agg(
            count("user").alias("total_edits"),
            approx_count_distinct("user").alias("unique_users"),
            concat_ws(", ", collect_set("user")).alias("combatants"),
            last("comment").alias("latest_comment"),
            last("revision_id").alias("latest_revision"),
            spark_sum("is_revert").alias("reverts_count"),
        )
        .filter((col("total_edits") >= 2) & (col("reverts_count") >= 1))
    )

    # 2. SUPER-EDYTORZY
    editors_agg = (
        parsed
        .withWatermark("timestamp", "5 minutes")
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("user"))
        .agg(
            count("*").alias("edit_count"),
            last("is_bot").alias("is_bot_flag"),
            last("is_suspicious").alias("is_suspicious_flag"),
            (spark_sum("length_diff") / count("*")).alias("avg_length_diff"),
            concat_ws(", ", collect_set("wiki_lang")).alias("langs_edited"),
        )
        .filter(col("edit_count") >= 3)
    )

    # 3. TRENDY TEMATYCZNE
    topics_agg = (
        parsed
        .withWatermark("timestamp", "5 minutes")
        .withColumn("topic", split(col("title"), r"[ _]")[0])
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("topic"))
        .agg(
            approx_count_distinct("title").alias("article_count"),
            count("*").alias("total_edits"),
            concat_ws(" | ", collect_set("title")).alias("sample_titles"),
        )
        .filter(col("article_count") >= 2)
    )

    # 4. MAPA — TYLKO eventy z lat/lon
    map_stream = parsed.filter(
        col("lat").isNotNull() & col("lon").isNotNull()
    )

    # ── QUERIES ──────────────────────────────
    q_wars = (
        wars_agg.writeStream
        .outputMode("update")
        .foreachBatch(process_edit_wars)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "_wars")
        .start()
    )
    q_editors = (
        editors_agg.writeStream
        .outputMode("update")
        .foreachBatch(process_super_editors)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "_editors")
        .start()
    )
    q_topics = (
        topics_agg.writeStream
        .outputMode("update")
        .foreachBatch(process_topic_trends)
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "_topics")
        .start()
    )
    q_map = (
        map_stream.writeStream
        .outputMode("append")
        .foreachBatch(update_live_map)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", CHECKPOINT_BASE + "_map")
        .start()
    )

    print(f"✓ Uruchomiono {len(session.streams.active)} strumieni. Ctrl+C aby zatrzymać.")
    session.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        start_app()
    except KeyboardInterrupt:
        print("\nZatrzymywanie Sparka …")
