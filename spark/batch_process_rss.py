import argparse
from datetime import datetime
import yaml

from loguru import logger
from operator import itemgetter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from udfs import clean_html_text, sentiment

def main(day):
    logger.info("Initializing spark session")
    spark = SparkSession \
        .builder \
        .appName("Batch Process rss") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    
    logger.info("Reading input data")
    rss_frame = spark.read.parquet("/user/nifi/rss")# \
        #.filter(col("day") == f"{day.year:04}-{day.month:02}-{day.day:02}")
    rss_frame = rss_frame.select(
        "title",
        "link",
        "description",
        "pubDate",
        "channel_code",
        "day"
    ).distinct()

    logger.info("Preprocessing text and sentiment")
    rss_frame = rss_frame \
        .withColumn("description", clean_html_text(col("description"))) \
        .withColumn("sentiment", sentiment(col("description")))

    logger.info("Output schema")
    rss_frame.printSchema()

    logger.info("Adding companies columns")
    logger.info("Processing config file")
    with open("config/companies_names.yaml") as f:
        companies_names = yaml.load(f, Loader=yaml.CLoader)
    companies_items = companies_names.items()
    companies_items = sorted(companies_items, key=itemgetter(0))
    
    logger.info("Adding companies mentions columns")
    for name, company_regex in companies_items:
        rss_frame = rss_frame.withColumn(
            f"is_{name}_mentioned",
            when(
                upper(col("description")).rlike(company_regex.upper()), 1
            ).otherwise(0)
        )
    
    logger.info("Output schema")
    rss_frame.printSchema()

    logger.info("Writing output")
    rss_frame \
        .select(
            "title",
            "link",
            "description",
            "pubDate",
            "sentiment",
            *[f"is_{name}_mentioned" for name, _ in companies_items],
            "day",
            "channel_code",
        ) \
        .write \
        .partitionBy("day", "channel_code") \
        .mode("append") \
        .saveAsTable("projekt.rss_with_companies_mentions")
    logger.info("Finished")

def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format. Please use 'yyyy-MM-dd'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and dump rss data for a given day")
    parser.add_argument(
        '--day',
        type=validate_date_format,
        default=datetime.today().strftime('%Y-%m-%d'),
        help='Date in the format yyyy-MM-dd'
    )

    args = parser.parse_args()

    main(datetime.strptime(args.day, '%Y-%m-%d'))