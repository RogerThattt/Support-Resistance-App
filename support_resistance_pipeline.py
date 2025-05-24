
# Databricks Notebook: Support-Resistance Option Chain Analytics Pipeline
# Author: Auto-generated
# Date: 2025-05-24

# COMMAND ----------

# 1. Setup Kafka Streaming Source
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Define schema for option chain data
option_schema = StructType([
    StructField("instrument", StringType(), True),
    StructField("strike", DoubleType(), True),
    StructField("expiry", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("last", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("open_interest", IntegerType(), True)
])

# Kafka configuration
kafka_bootstrap_servers = "your-kafka-bootstrap-server:9092"
kafka_topic = "option-chain-stream"

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = raw_df.select(from_json(col("value").cast("string"), option_schema).alias("data")).select("data.*")

# COMMAND ----------

# 2. Partitioning & Metadata Enrichment
from pyspark.sql.functions import year, month, dayofmonth, hour

enriched_df = parsed_df.withColumn("year", year("timestamp")) \
                       .withColumn("month", month("timestamp")) \
                       .withColumn("day", dayofmonth("timestamp")) \
                       .withColumn("hour", hour("timestamp"))

# COMMAND ----------

# 3. Write to Delta Table (Bronze Layer)
bronze_path = "/mnt/datalake/option_chain/bronze"

(
    enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_path + "/_checkpoint")
    .partitionBy("instrument", "year", "month", "day")
    .start(bronze_path)
)

# COMMAND ----------

# 4. Ad-hoc Query for Validation
# NOTE: Run this in a separate cell when data is available.
# Replace instrument_name and date as required

from delta.tables import DeltaTable

display(spark.sql("""
SELECT *
FROM delta.`/mnt/datalake/option_chain/bronze`
WHERE instrument = 'NIFTY'
  AND year = 2025 AND month = 5 AND day = 24
ORDER BY timestamp DESC
LIMIT 100
"""))

# COMMAND ----------

# Next Steps:
# - Create Silver layer: clean data, remove duplicates, compute mid-prices
# - Gold layer: calculate support/resistance levels, implied volatility surfaces
# - Feature store: generate ML features for predictive models
# - Dashboards: visualize trends, signals, anomalies using Databricks SQL or dashboards
