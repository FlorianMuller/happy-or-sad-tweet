# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.kafka:kafka-clients:2.7.0,org.mongodb.spark:mongo-spark-connector:10.0.2 consumer.py
# org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
# org.apache.kafka:kafka-clients:2.7.0
# org.mongodb.spark:mongo-spark-connector:10.0.2
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import eng_spacysentiment
from afinn import Afinn

# Initialize Afinn Engine
afinn = Afinn()

# Load Spacy NLP english engine
#spacy_nlp = eng_spacysentiment.load()

# Create a SentimentIntensityAnalyzer object.
#sid_obj = SentimentIntensityAnalyzer()
 

# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"

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
 

# Compute vader sentiment version
def vader_sentiment_scores(sentence):
    sid_obj = SentimentIntensityAnalyzer()
    sentiment_dict = sid_obj.polarity_scores(sentence)

#    if sentiment_dict['compound'] >= 0.05 :
#        feeling ="Positive"
#    elif sentiment_dict['compound'] <= - 0.05 :
#        feeling = "Negative"
#    else :
#        feeling = "Neutral"
#
#    return sentiement_dict, feeling
    return sentiment_dict


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


def simplify_sentiment_vader(sentiment_scores):
    if sentiment_scores['compound'] >= 0.05 :
        return "Positive"
    elif sentiment_scores['compound'] <= - 0.05 :
        return "Negative"
    else :
        return "Neutral"


# Creating spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .config("spark.mongodb.read.connection.uri", "mongodb://root:example@localhost:27017/ridiculus_elephant") \
    .config("spark.mongodb.write.connection.uri", "mongodb://root:example@localhost:27017/ridiculus_elephant") \
    .appName("StreamingThings") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


#kafka_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option("subscribe", TOPIC)
#
#kafka_df = kafka_stream \
#  .load()


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


# Creating spark user function
udf_vader_sentiment_scores = f.udf(vader_sentiment_scores, returnType=StructType([
    StructField("pos", FloatType(), False),
    StructField("compound", FloatType(), False),
    StructField("neu", FloatType(), False),
    StructField("neg", FloatType(), False),
]))
udf_simplify_sentiment = f.udf(simplify_sentiment_vader, returnType=StringType())


udf_spacy_sentiment_scores = f.udf(spacy_sentiment_scores, returnType=StructType([
    StructField("positive", FloatType(), False),
    #StructField("compound", FloatType(), False),
    StructField("negative", FloatType(), False)
]))
udf_afinn_sentiment_scores = f.udf(afinn_sentiment_scores, returnType=StructType([
    StructField("score", FloatType(), False),
    #StructField("compound", FloatType(), False),
    #StructField("neu", FloatType(), False),
    #StructField("neg", FloatType(), False)
]))

   
udf_simplify_spacy = f.udf(simplify_sentiment_spacy, returnType=StringType())
udf_simplify_afinn = f.udf(simplify_sentiment_afinn, returnType=StringType())

json_df = json_df \
    .withColumn("details_feeling", udf_vader_sentiment_scores("text")) \
    .withColumn("overall_feeling", udf_simplify_sentiment("details_feeling")) \
    .withColumn("details_spacy", udf_spacy_sentiment_scores("text")) \
    .withColumn("spacy_feeling", udf_simplify_spacy("details_spacy")) \
    .withColumn("details_afinn", udf_afinn_sentiment_scores("text")) \
    .withColumn("afinn_feeling", udf_simplify_afinn("details_afinn"))

#    tweet["details"], tweet["overall_feeling"] = vader_sentiment(sid_obj, tweet['text'])
#    tweet["details_spacy"], tweet["spacy_feeling"] = spacy_sentiment(spacy_nlp, tweet['text'])
#    tweet["details_afinn"], tweet["afinn_feeling"] = afinn_sentiment(afinn, tweet['text'])

print(f"QUERY WRITE TO {TOPIC} =============================================================")
# Sarting streaming to mongodb
query = json_df \
    .writeStream \
    .format("mongodb") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("checkpointLocation", "/tmp/spark-checkpoint2") \
    .option("spark.mongodb.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.database", "ridiculus_elephant") \
    .option("spark.mongodb.collection", TOPIC) \
    .outputMode("append") \
    .start()
query.awaitTermination()

    #.trigger(continuous="1 second") \
    #.option("spark.mongodb.connection.uri", "mongodb://root:example@localhost:27017") \
# Sarting streaming to the console (debuging)
#query = (json_df
#   .writeStream
#   .format("console")
#   #.trigger(continuous="10 seconds")
#   .outputMode("append")
#   .foreachBatch(saveToMongo)
#   ) \
#   .start()
#query.awaitTermination() 

   #.trigger(continuous="1 second") \


# Lazily instantiated global instance of SparkSession
# def getSparkSessionInstance(sparkConf):
#     if ("sparkSessionSingletonInstance" not in globals()):
#         # For local testing and unit tests, you can pass “local[*]” to run Spark Streaming in-process (detects the number of cores in the local system)
#         globals()["sparkSessionSingletonInstance"] = SparkSession \
#             .builder \
#             .config(conf=sparkConf) \
#             .getOrCreate()
#     return globals()["sparkSessionSingletonInstance"]

# def func_call(df, batch_id):
#     df.selectExpr("CAST(value AS STRING) as json")
#     requests = df.rdd.map(lambda x: x.value).collect()
#     print(requests)

    # .foreachBatch(func_call) \
    # .trigger(processingTime="5") \
    # .start() \
    # .awaitTermination()

# .format(HiveWarehouseSession.STREAM_TO_STREAM) \
# .option("checkpointLocation","file://F:/tmp/kafka/checkpoint") \

# s.pprint()
# ssc.start()             # Start the computation
# ssc.awaitTermination()  # Wait for the computation to terminate
