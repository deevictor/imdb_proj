import os, glob, shutil
from datetime import datetime
from pyspark.sql.functions import col, avg, count, round as spark_round

RATING_STREAM_DIR = "data/ratings_stream"
CANDIDATE_PATH = "output/candidates"
TOP10_STREAM_SINK = "output/top10_stream"

os.makedirs(CANDIDATE_PATH, exist_ok=True)
os.makedirs(TOP10_STREAM_SINK, exist_ok=True)

def save_top10(df, batch_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(TOP10_STREAM_SINK, f"top10_batch_{batch_id}_{timestamp}.tsv")
    df.coalesce(1).write.mode("overwrite") \
        .option("header", True).option("sep", "\t").csv("top10_temp")
    part_file = glob.glob("top10_temp/part-*.csv")[0]
    shutil.move(part_file, output_path)
    shutil.rmtree("top10_temp")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df = batch_df.filter(col("numVotes") >= 500).repartition(50)
    batch_df.write.mode("append").parquet(CANDIDATE_PATH)
    candidate_df = batch_df.sparkSession.read.parquet(CANDIDATE_PATH).repartition(100)

    stats = candidate_df.agg(
        avg("numVotes").alias("global_avg"),
        count("numVotes").alias("total_count")
    ).collect()[0]

    global_avg = stats["global_avg"]
    total_count = stats["total_count"]
    if global_avg is None or total_count == 0:
        return

    ranked = candidate_df.withColumn(
        "ranking",
        spark_round((col("numVotes") / global_avg) * col("averageRating"), 2)
    )

    top10 = ranked.orderBy(col("ranking").desc()).limit(10)
    save_top10(top10, batch_id)

    print(f"\n=== BATCH {batch_id} ===")
    print(f"Global avgVotes: {global_avg:.2f} (records: {total_count})")
    top10.select("tconst", "averageRating", "numVotes", "ranking").show(truncate=False)

def start_rating_stream(spark):
    from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

    ratings_schema = StructType() \
        .add("tconst", StringType()) \
        .add("averageRating", FloatType()) \
        .add("numVotes", IntegerType())

    rating_stream = spark.readStream \
        .schema(ratings_schema) \
        .option("sep", "\t") \
        .option("header", "true") \
        .option("checkpointLocation", "checkpoints/weighted_avg_stream") \
        .csv(RATING_STREAM_DIR)

    return rating_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "checkpoints/weighted_avg_stream") \
        .start()