import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import eng_spacysentiment
from afinn import Afinn

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

# Compute Afinn sentiment version
def afinn_sentiment_scores(text):
  afinn = Afinn()
  score = afinn.score(text)
#  if abs(score) < 0.5:
#    # Neutral (=> between -0.5 and 0.5, through abs function)
#    feeling = "Neutral"
#  elif score > 0.5:
#    # Positive
#    feeling = "Positive"
#  else:
#    # Negative
#    feeling = "Negative"

#  return score, feeling
  return {"score":score}

# Compute spacy sentiment version
def spacy_sentiment_scores(text):
  spacy_nlp = eng_spacysentiment.load()
  doc = spacy_nlp(text)
#  if doc.cats["positive"] > doc.cats["negative"]:
#    feeling = "Positive"
#  elif doc.cats["negative"] > doc.cats["positive"]:
#    feeling = "Negative"
#  else:
#    feeling = "Neutral"

#  return doc.cats, feeling
  return doc.cats



def simplify_sentiment(sentiment_scores):
    if sentiment_scores['compound'] >= 0.05 :
        return "Positive"
    elif sentiment_scores['compound'] <= - 0.05 :
        return "Negative"
    else :
        return "Neutral"

def simplify_sentiment_spacy(scores):
  if scores["positive"] > 0.4 and scores["positive"] < 0.6 and scores["negative"] > 0.4 and scores["negative"] < 0.6:
    feeling = "Neutral"
  if scores["positive"] > scores["negative"]:
    feeling = "Positive"
  elif scores["negative"] > scores["positive"]:
    feeling = "Negative"
  else:
    feeling = "Neutral"
  return feeling


def simplify_sentiment_afinn(scores):
  scores = scores["score"]
  if abs(scores) < 0.5:
    # Neutral (=> between -0.5 and 0.5, through abs function)
    feeling = "Neutral"
  elif scores > 0.5:
    # Positive
    feeling = "Positive"
  else:
    # Negative
    feeling = "Negative"
  return feeling

def add_sentiment(df):
    # Creating spark user define function (udf)
    udf_sentiment_scores = f.udf(sentiment_scores, returnType=StructType([
        StructField("pos", FloatType(), False),
        StructField("compound", FloatType(), False),
        StructField("neu", FloatType(), False),
        StructField("neg", FloatType(), False),
    ]))
    udf_simplify_sentiment = f.udf(simplify_sentiment, returnType=StringType())
    
    udf_spacy_sentiment_scores = f.udf(spacy_sentiment_scores, returnType=StructType([
        StructField("positive", FloatType(), False),
        StructField("negative", FloatType(), False)
    ]))
    
    udf_afinn_sentiment_scores = f.udf(afinn_sentiment_scores, returnType=StructType([
        StructField("score", FloatType(), False),
    ]))


    udf_simplify_spacy = f.udf(simplify_sentiment_spacy, returnType=StringType())
    udf_simplify_afinn = f.udf(simplify_sentiment_afinn, returnType=StringType())

    return df \
        .withColumn("details_feeling", udf_sentiment_scores("text")) \
        .withColumn("overall_feeling", udf_simplify_sentiment("details_feeling")) \
        .withColumn("details_spacy", udf_spacy_sentiment_scores("text")) \
        .withColumn("spacy_feeling", udf_simplify_spacy("details_spacy")) \
        .withColumn("details_afinn", udf_afinn_sentiment_scores("text")) \
        .withColumn("afinn_feeling", udf_simplify_afinn("details_afinn"))



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
    #time.sleep(45)
    main()
