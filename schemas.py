from pyspark.sql.types import StructType, StringType

names_schema = StructType() \
        .add("nconst", StringType()) \
        .add("primaryName", StringType()) \
        .add("birthYear", StringType()) \
        .add("deathYear", StringType()) \
        .add("primaryProfession", StringType()) \
        .add("knownForTitles", StringType())