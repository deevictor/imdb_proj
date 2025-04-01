import os
from pyspark.sql.types import StructType, StringType

from schemas import names_schema

# === Config Paths ===
NAMES_INPUT_DIR = "data/names_stream"
NAMES_STREAM_SINK = "output/names_table"
NAMES_CHECKPOINT_DIR = "checkpoints/names_stream"

# Ensure directories exist
os.makedirs(NAMES_INPUT_DIR, exist_ok=True)
os.makedirs(NAMES_STREAM_SINK, exist_ok=True)

# === Batch Logic ===
def process_names_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] No names data to process.")
        return

    count = batch_df.count()
    print(f"[Batch {batch_id}] Processing {count} name records...")

    # ✅ Repartition to reduce memory pressure
    batch_df = batch_df.repartition(20)

    batch_df.write \
        .mode("append") \
        .parquet(NAMES_STREAM_SINK)

    print(f"[Batch {batch_id}] ✅ Done writing {count} name rows to Parquet.")


# === Streaming Entrypoint ===
def start_names_stream(spark):
    return spark.readStream \
        .option("sep", "\t") \
        .option("header", True) \
        .option("maxFilesPerTrigger", 1) \
        .schema(names_schema) \
        .csv(NAMES_INPUT_DIR) \
        .writeStream \
        .foreachBatch(process_names_batch) \
        .outputMode("append") \
        .option("checkpointLocation", NAMES_CHECKPOINT_DIR) \
        .start()