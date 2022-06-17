import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# take environment variables from .env
load_dotenv()
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"


def init_spark():
    spark = (
        SparkSession.builder.master(os.environ["SPARK_MASTER"])
        .appName("twitter_sentiment")
        .config("spark.mongodb.read.connection.uri", f"{os.environ['MONGO_URI']}/{os.environ['MONGO_DB']}")
        .config("spark.mongodb.write.connection.uri", f"{os.environ['MONGO_URI']}/{os.environ['MONGO_DB']}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    return spark


def read_stream_from_kafka(spark):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BROKERS"]) \
        .option("subscribe", TOPIC) \
        .load()


def json_string_to_df(df):
    json_schema = StructType([
        StructField("id", StringType(), False),
        StructField("text", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("author_id", StringType(), False),
        StructField("lang", StringType(), True),
        # Todo: add more fields from twitter api
        # StructField("geo", StructType([
        # ]), True),
        # StructField("entities", StructType([
        # ]), True),
        # StructField("attachments", StructType([
        # ]), True),
    ])

    return df.withColumn('value', f.from_json(f.col('value'), json_schema)).select(f.col("value.*"))


def sentiment_scores(sentence):
    sid_obj = SentimentIntensityAnalyzer()
    return sid_obj.polarity_scores(sentence)


def simplify_sentiment(sentiment_scores):
    if sentiment_scores['compound'] >= 0.05 :
        return "Positive"
    elif sentiment_scores['compound'] <= - 0.05 :
        return "Negative"
    else :
        return "Neutral"


def add_sentiment(df):
    # Creating spark user define function (udf)
    udf_sentiment_scores = f.udf(sentiment_scores, returnType=StructType([
        StructField("pos", FloatType(), False),
        StructField("compound", FloatType(), False),
        StructField("neu", FloatType(), False),
        StructField("neg", FloatType(), False),
    ]))
    udf_simplify_sentiment = f.udf(simplify_sentiment, returnType=StringType())

    return df \
        .withColumn("details_feeling", udf_sentiment_scores("text")) \
        .withColumn("overall_feeling", udf_simplify_sentiment("details_feeling"))


def write_stream_to_mongo(df):
    # Stream to console (for debugging)
    # return df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    return df \
        .writeStream \
        .option("checkpointLocation", "/tmp/spark-checkpoint2") \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://root:example@mongo:27017") \
        .option("spark.mongodb.database", "ridiculus_elephant") \
        .option("spark.mongodb.collection", TOPIC) \
        .outputMode("append") \
        .start()


def main():
    # Creating spark session
    spark = init_spark()
    
    # Streaming data from kafka
    kafka_df = read_stream_from_kafka(spark)

    # Convert kafka json string to dataframe
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
    json_df = json_string_to_df(kafka_df)

    # Sentiment analysis
    json_df = add_sentiment(json_df)

    # Send stream to mongoDB
    query = write_stream_to_mongo(json_df)
    query.awaitTermination()

if __name__ == "__main__":
    time.sleep(45)
    main()
