import os
from pyspark.sql.types import StructType, StringType, IntegerType

# === Config Paths ===
ALIAS_INPUT_DIR = "data/titles_alias_stream"
ALIAS_STREAM_SINK = "output/titles_alias_table"
ALIAS_CHECKPOINT_DIR = "checkpoints/alias_stream"

# Ensure directories exist
os.makedirs(ALIAS_INPUT_DIR, exist_ok=True)
os.makedirs(ALIAS_STREAM_SINK, exist_ok=True)


# === Batch Processing Logic ===
def process_alias_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] No alias data to process.")
        return

    count = batch_df.count()
    print(f"[Batch {batch_id}] Processing {count} alias rows...")

    # ✅ Repartition to avoid OOM
    batch_df = batch_df.repartition(20)

    batch_df.write \
        .mode("append") \
        .parquet(ALIAS_STREAM_SINK)

    print(f"[Batch {batch_id}] ✅ Done writing {count} rows to alias table.")


# === Streaming Entrypoint ===
def start_alias_stream(spark):
    alias_schema = StructType() \
        .add("titleId", StringType()) \
        .add("ordering", IntegerType()) \
        .add("title", StringType()) \
        .add("region", StringType()) \
        .add("language", StringType()) \
        .add("types", StringType()) \
        .add("attributes", StringType()) \
        .add("isOriginalTitle", StringType())

    return spark.readStream \
        .option("sep", "\t") \
        .option("header", True) \
        .option("maxFilesPerTrigger", 1) \
        .schema(alias_schema) \
        .csv(ALIAS_INPUT_DIR) \
        .writeStream \
        .foreachBatch(process_alias_batch) \
        .outputMode("append") \
        .option("checkpointLocation", ALIAS_CHECKPOINT_DIR) \
        .start()