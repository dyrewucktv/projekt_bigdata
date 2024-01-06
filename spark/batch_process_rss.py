import argparse
from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparknlp.annotator import Tokenizer, WordEmbeddingsModel, NerDLModel, NerConverter, UniversalSentenceEncoder, \
    SentimentDLModel
from sparknlp.base.document_assembler import DocumentAssembler


def get_document(df):
    document_assembler = DocumentAssembler().setInputCol("description").setOutputCol("document")
    return document_assembler.transform(df)


def get_ner(df):
    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")
    embeddings = WordEmbeddingsModel \
        .pretrained("glove_100d") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings")

    ner_model = NerDLModel \
        .pretrained("onto_100", "en") \
        .setInputCols(["document", "token", "embeddings"]) \
        .setOutputCol("ner")

    ner_converter = NerConverter() \
        .setInputCols(["document", "token", "ner"]) \
        .setOutputCol("ner_chunk")

    pipeline = Pipeline(
        stages=[
            tokenizer,
            embeddings,
            ner_model,
            ner_converter
        ]
    )

    result = pipeline.fit(df).transform(df)
    return result


def get_sentiment(df):
    encoder = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en") \
        .setInputCols(["document"]) \
        .setOutputCol("sentence_embeddings")
    sentimentdl = SentimentDLModel.pretrained(name="sentimentdl_use_twitter", lang="en") \
        .setInputCols(["sentence_embeddings"]) \
        .setOutputCol("sentiment")
    pipeline = Pipeline(
        stages=[
            encoder,
            sentimentdl
        ])
    result = pipeline.fit(df).transform(df)
    return result


def main(day):
    spark = SparkSession \
        .builder \
        .appName("Batch Process rss") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.2.2") \
        .getOrCreate()

    df = spark \
        .read \
        .options(inferSchema=True) \
        .parquet("/user/nifi/rss/") \
        .filter(col("day") == f"{day.year:04}-{day.month:02}-{day.day:02}")
    df = get_document(df)
    df = get_sentiment(df)
    df = get_ner(df)
    df.show(1,vertical=True,truncate=False)
    df.write.partitionBy(["day", "channel_code"]).parquet("/user/spark/rss_processed")


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
