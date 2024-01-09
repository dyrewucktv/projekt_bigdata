from pyspark.sql import SparkSession, functions as psFunc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("volume", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("dividends", StringType(), True),
    StructField("stock_splits", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("time", StringType(), True),
    StructField("close", StringType(), True),
    StructField("open", StringType(), True),
])




def main():
    spark = SparkSession \
        .builder \
        .appName("Spark stream process financial") \
        .getOrCreate()

    streamingdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("kafka.enable.autocommit", "true") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "yfinance") \
        .load()

    value_df = streamingdf \
        .selectExpr("CAST(value AS STRING)")

    parsed_df = value_df.select(psFunc.from_json(psFunc.col("value"), schema).alias("data")).select("data.*")

    converted_df = parsed_df \
        .withColumn("volume", psFunc.col("volume").cast(DoubleType())) \
        .withColumn("high", psFunc.col("high").cast(DoubleType())) \
        .withColumn("low", psFunc.col("low").cast(DoubleType())) \
        .withColumn("dividends", psFunc.col("dividends").cast(DoubleType())) \
        .withColumn("stock_splits", psFunc.col("stock_splits").cast(DoubleType())) \
        .withColumn("time", psFunc.col("time").cast(TimestampType())) \
        .withColumn("close", psFunc.col("close").cast(DoubleType())) \
        .withColumn("open", psFunc.col("open").cast(DoubleType()))

    result_df = converted_df \
        .withWatermark("time", "5 minutes") \
        .groupBy("company_name", psFunc.window("time", "5 minutes")) \
        .agg(
        psFunc.stddev_pop("high").alias("volatility"),
        psFunc.count("time").alias("count"),
        psFunc.min("low").alias("min_min"),
        psFunc.max("high").alias("high_high"),
        psFunc.sum("volume").alias("total_volume")
    )

    query = result_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("kafka.enable.autocommit", "true") \
        .option("checkpointLocation", "hdfs://node1/user/spark/checkpoints/") \
        .option("topic", "financial_windowed") \
        .start()
    try:
        query.awaitTermination()
        query.stop()
        spark.stop()
    except:
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main()
