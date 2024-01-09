import yaml

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logger.info("Initializing spark session")
spark = SparkSession \
    .builder \
    .appName("Batch yfinance") \
    .master("local[2]") \
    .enableHiveSupport() \
    .getOrCreate()

yfinance_history = spark.read.parquet("/user/vagrant/finance/history").where("Date >= '2024-01-08'")

yfinance_history_aggregated = yfinance_history \
    .orderBy("Time") \
    .groupBy("Date", "Company") \
    .agg({
        "Open": "first",
        "Close": "last",
        "Low": "min",
        "High": "max"
    }).select(
        col("Date"),
        col("Company"),
        col("max(High)").alias("High"),
        col("first(Open)").alias("Open"),
        col("last(Close)").alias("Close"),
        col("min(Low)").alias("Low"),
    )

rss = spark \
    .sql("SELECT * FROM projekt.rss_with_companies_mentions") \
    .withColumnRenamed("day", "RssDate")

with open("config/companies_aliases.yaml") as f:
    companies_aliases = yaml.load(f, Loader=yaml.CLoader)

join_condition = lit(1) != lit(1)
for company_name, feature_alias in companies_aliases.items():
    join_condition = (
        join_condition
        | (
            (yfinance_history_aggregated.Company == company_name)
            & (getattr(rss, f"is_{feature_alias}_mentioned") == 1)
        )
    )
joined_data = yfinance_history_aggregated \
    .join(
        rss,
        join_condition 
        & (rss.RssDate == yfinance_history_aggregated.Date),
        "left"
    ) \
    .withColumn("partition_by_date", col("Date")) \
    .withColumn("partition_by_company", col("Company"))

joined_data \
    .write \
    .partitionBy("partition_by_date", "partition_by_company") \
    .mode("overwrite") \
    .saveAsTable("projekt.yfinance_with_rss")
logger.info("Finished")