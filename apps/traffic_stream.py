from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType

# Create Spark Session
spark = SparkSession.builder \
    .appName("SmartCityTrafficStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON messages
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("avg_speed", IntegerType())

# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka 'value' from binary to JSON
traffic_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp string to timestamp type (Event Time)
traffic_df = traffic_df.withColumn(
    "event_time",
    col("timestamp").cast("timestamp")
)

# 5-minute tumbling window aggregation
windowed_traffic = traffic_df.groupBy(
    window(col("event_time"), "5 minutes"),
    col("sensor_id")
).avg("vehicle_count", "avg_speed")

# Detect congestion (real-time alert)
alerts_df = traffic_df.filter(col("avg_speed") < 10)

# Output alerts immediately (console for demo)
alert_query = alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Output windowed aggregation (console for demo)
aggregation_query = windowed_traffic.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()
