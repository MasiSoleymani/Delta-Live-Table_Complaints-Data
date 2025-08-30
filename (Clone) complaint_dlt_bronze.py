import dlt
from pyspark.sql.functions import current_timestamp, col

SOURCE_PATH = "/Volumes/complaints/bronze/my_volume"  # <-- no /my_data since your file is in the root

@dlt.table(
    name="complaints_bronze_raw",
    comment="Raw monthly complaint CSVs from Volume (append-only)."
)
def bronze():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("rescuedDataColumn", "_rescued_data")
          .load(SOURCE_PATH))
    return (df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_source_file_mtime", col("_metadata.file_modification_time")))