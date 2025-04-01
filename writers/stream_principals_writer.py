import os
from pyspark.sql.types import StructType, StringType, IntegerType

# === Config Paths ===
PRINCIPALS_INPUT_DIR = "data/titles_principals_stream"
PRINCIPALS_STREAM_SINK = "output/titles_principals_table"
PRINCIPALS_CHECKPOINT_DIR = "checkpoints/principals_stream"

# Ensure directories exist
os.makedirs(PRINCIPALS_INPUT_DIR, exist_ok=True)
os.makedirs(PRINCIPALS_STREAM_SINK, exist_ok=True)


# === Batch logic ===
def process_principals_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] No data to process.")
        return

    count = batch_df.count()
    print(f"[Batch {batch_id}] Processing {count} rows...")

    # ✅ Repartition to avoid OOM during write
    batch_df = batch_df.repartition(20)

    batch_df.write \
        .mode("append") \
        .parquet(PRINCIPALS_STREAM_SINK)

    print(f"[Batch {batch_id}] ✅ Done writing {count} rows to Parquet.")


# === Stream entrypoint ===
def start_principals_stream(spark):
    principals_schema = StructType() \
        .add("tconst", StringType()) \
        .add("ordering", IntegerType()) \
        .add("nconst", StringType()) \
        .add("category", StringType()) \
        .add("job", StringType()) \
        .add("characters", StringType())

    return spark.readStream \
        .option("sep", "\t") \
        .option("header", True) \
        .option("maxFilesPerTrigger", 1) \
        .schema(principals_schema) \
        .csv(PRINCIPALS_INPUT_DIR) \
        .writeStream \
        .foreachBatch(process_principals_batch) \
        .outputMode("append") \
        .option("checkpointLocation", PRINCIPALS_CHECKPOINT_DIR) \
        .start()