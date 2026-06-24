"""
Structured Streaming 实时处理数据
消费 Kafka sex → 1秒窗口聚合 → 写入 result
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, concat, lit, to_json, struct

spark = SparkSession.builder \
    .appName("KafkaWordCount") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sex") \
    .load() \
    .selectExpr("CAST(value AS STRING) as gender", "timestamp")

filtered = df.filter(col("gender").isin("0", "1"))

result = filtered.withWatermark("timestamp", "3 seconds") \
    .groupBy(
        window(col("timestamp"), "3 seconds", "3 seconds"),
        col("gender")
    ).count() \
    .select(
        to_json(struct(col("gender"), col("count"))).alias("value")
    )

query = result.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "result") \
    .option("checkpointLocation", "/tmp/ss_checkpoint") \
    .trigger(processingTime="3 seconds") \
    .start()

print("Structured Streaming started...")
query.awaitTermination()
