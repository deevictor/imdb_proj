import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

from schemas import names_schema

TOP10_STREAM_DIR = "output/top10_stream/"
ALIAS_PARQUET_DIR = "output/titles_alias_table/"
PRINCIPALS_PARQUET_DIR = "output/titles_principals_table/"
NAMES_PARQUET_DIR = "output/names_table/"
CHECKPOINT_DIR = "checkpoints/top10_combined_enricher"


def process_combined_enrichment(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] ❌ Empty batch. Skipping.")
        return

    spark = batch_df.sparkSession

    # еop 10 tconsts
    tconst_list = [row["tconst"] for row in batch_df.select("tconst").distinct().collect()]
    if not tconst_list:
        print(f"[Batch {batch_id}]  No tconsts found in top10.")
        return

    print(f"[Batch {batch_id}] 🎬 Top 10 Movie IDs:")
    print(tconst_list)

    # Enrich with aliases
    alias_df = spark.read.parquet(ALIAS_PARQUET_DIR)
    filtered_alias_df = alias_df.filter(col("titleId").isin(tconst_list))

    print(f"[Batch {batch_id}] Alternate titles for top10 movies:")
    filtered_alias_df.select(
        "titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"
    ).show(truncate=False, n=filtered_alias_df.count())

    # Enrich with principal info
    principals_df = spark.read.parquet(PRINCIPALS_PARQUET_DIR)
    filtered_principals = principals_df.filter(col("tconst").isin(tconst_list))

    print(f"[Batch {batch_id}]  Principals linked to top10 movies:")
    filtered_principals.select("tconst", "nconst", "category", "job").show(truncate=False, n=20)

    counts = filtered_principals.select("tconst", "nconst").dropDuplicates() \
        .groupBy("nconst").count().orderBy(desc("count"))

    if counts.isEmpty():
        print(f"[Batch {batch_id}] ❌ No matching principals found.")
        return

    print(f"[Batch {batch_id}] Top 10 most credited people in top10 movies:")
    counts.select("nconst", "count").show(n=10, truncate=False)

    # Top principal details
    top_person_row = counts.select("nconst", "count").limit(1).collect()
    if not top_person_row:
        print(f"[Batch {batch_id}] ❌ No top principal found.")
        return

    nconst_value = top_person_row[0]["nconst"]
    top_person_df = spark.read \
        .schema(names_schema) \
        .parquet(NAMES_PARQUET_DIR) \
        .filter(col("nconst") == nconst_value)

    print(f"[Batch {batch_id}] Most credited person details:")
    top_person_df.show(truncate=False)


def start_top10_enrichment_stream(spark):
    top10_schema = StructType() \
        .add("tconst", StringType()) \
        .add("averageRating", FloatType()) \
        .add("numVotes", IntegerType()) \
        .add("ranking", FloatType())

    top10_stream = spark.readStream \
        .option("sep", "\t") \
        .option("header", True) \
        .schema(top10_schema) \
        .csv(TOP10_STREAM_DIR)

    return top10_stream.writeStream \
        .foreachBatch(process_combined_enrichment) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()
