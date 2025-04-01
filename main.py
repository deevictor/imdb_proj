from pyspark.sql import SparkSession

from chained_streams.most_credited_and_aliases import start_top10_enrichment_stream
from writers.stream_alias_writer import start_alias_stream
from writers.stream_names_writer import start_names_stream
from writers.stream_principals_writer import start_principals_stream
from chained_streams.stream_ratings import start_rating_stream


spark = SparkSession.builder \
    .appName("Top10Movies") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

alias_query = start_alias_stream(spark)
principals_query = start_principals_stream(spark)
names_query = start_names_stream(spark)
rating_query = start_rating_stream(spark)
enricher_query = start_top10_enrichment_stream(spark)


queries = [alias_query, principals_query, names_query, rating_query, enricher_query]

try:
    for q in queries:
        q.awaitTermination()
except KeyboardInterrupt:
    print("Stopping all streams...")
    for q in queries:
        q.stop()
        print(f"Stopped query: {q.id}")
finally:
    spark.stop()
    print("Spark session stopped.")
