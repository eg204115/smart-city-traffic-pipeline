from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window
)
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("SmartCityTrafficStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for incoming Kafka JSON
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("avg_speed", IntegerType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .load()

# Parse JSON
traffic_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.sensor_id"),
    col("data.vehicle_count"),
    col("data.avg_speed"),
    col("data.timestamp").cast(TimestampType()).alias("event_time")
)

# 5-minute tumbling window aggregation
windowed_df = traffic_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        {"vehicle_count": "sum", "avg_speed": "avg"}
    ) \
    .withColumnRenamed("sum(vehicle_count)", "total_vehicles") \
    .withColumnRenamed("avg(avg_speed)", "avg_speed")

# Detect critical traffic
critical_df = windowed_df.filter(col("avg_speed") < 10)

# Output alerts to console (for demo)
alert_query = critical_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Store all processed data (Parquet)
data_query = windowed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/traffic_data") \
    .option("checkpointLocation", "output/checkpoints") \
    .start()

spark.streams.awaitAnyTermination()
