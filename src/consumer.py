import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

time.sleep(45)
# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"


# Creating spark session
spark = (
    SparkSession.builder.master("spark://spark:7077")
    .appName("twitter_sentiment")
    .config("spark.mongodb.read.connection.uri", "mongodb://root:example@mongo:27017/ridiculus_elephant") \
    .config("spark.mongodb.write.connection.uri", "mongodb://root:example@mongo:27017/ridiculus_elephant") \
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
  .option("subscribe", TOPIC) \
  .load()

kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# json_schema = spark.read.json(kafka_df).schema
json_schema = StructType([
    StructField("id", StringType(), False),
    StructField("text", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("author_id", StringType(), False),
    StructField("lang", StringType(), True),
    # StructField("geo", StructField([
    # ]), True),
    # StructField("entities", StructField([
    # ]), True),
    # StructField("attachments", StructField([
    # ]), True),
])
json_df = kafka_df.withColumn('value', f.from_json(f.col('value'), json_schema)).select(f.col("value.*"))

def sentiment_scores(sentence):
    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()
    return sid_obj.polarity_scores(sentence)


def simplify_sentiment(sentiment_scores):
    if sentiment_scores['compound'] >= 0.05 :
        return "Positive"
    elif sentiment_scores['compound'] <= - 0.05 :
        return "Negative"
    else :
        return "Neutral"

# Creating spark user function
udf_sentiment_scores = f.udf(sentiment_scores, returnType=StructType([
    StructField("pos", FloatType(), False),
    StructField("compound", FloatType(), False),
    StructField("neu", FloatType(), False),
    StructField("neg", FloatType(), False),
]))
udf_simplify_sentiment = f.udf(simplify_sentiment, returnType=StringType())

json_df = json_df \
    .withColumn("details_feeling", udf_sentiment_scores("text")) \
    .withColumn("overall_feeling", udf_simplify_sentiment("details_feeling"))


# Starting streaming to mongodb
query = json_df \
    .writeStream \
    .option("checkpointLocation", "/tmp/spark-checkpoint2") \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://root:example@mongo:27017") \
    .option("spark.mongodb.database", "ridiculus_elephant") \
    .option("spark.mongodb.collection", TOPIC) \
    .outputMode("append") \
    .start()
query.awaitTermination()