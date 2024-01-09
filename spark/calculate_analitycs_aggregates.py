import yaml

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logger.info("Initializing spark session")
spark = SparkSession \
    .builder \
    .appName("Calculate aggregates") \
    .master("local[2]") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("SELECT * FROM projekt.yfinance_with_rss")

aggregates_rss = df.groupBy(
    "date", "company"
).agg({
    "sentiment": "mean",
    "description": "count"
}).withColumnRenamed(
    "avg(sentiment)", "mean_sentiment"
).withColumnRenamed(
    "count(description)", "mention_count"
).filter("mention_count > 0")

aggregates_rss.write.mode("overwrite").saveAsTable("projekt.yfinance_aggregates_rss")

aggregates_financial = df \
    .select(
        col("Company"),
        col("Date"),
        col("Open"),
        col("Close"),
        col("Low"),
        col("High"),
        (col("Close") - col("Open")).alias("CloseOpenDifference"),
        (col("High") - col("Low")).alias("HighMinDifference"),
    ).distinct()

aggregates_financial.write.mode("overwrite").saveAsTable("projekt.yfinance_aggregates_financial")