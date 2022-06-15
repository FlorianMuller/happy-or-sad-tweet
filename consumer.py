import os
import time
import json
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
# import hdfs

# Waiting for zookeeper and kafka to start (takes ~ 10 sec)
time.sleep(45)

# import logging
# logging.basicConfig(level=logging.DEBUG)

# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"

def sentiment_scores(sentence):
    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()
 
    sentiment_dict = sid_obj.polarity_scores(sentence)
 
    if sentiment_dict['compound'] >= 0.05 :
        feeling ="Positive"
    elif sentiment_dict['compound'] <= - 0.05 :
        feeling = "Negative"
    else :
        feeling = "Neutral"
        
    return sentiment_dict, feeling


# client = hdfs.InsecureClient(os.environ["HDFS_URL"])

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    group_id='users-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Creation du fichier s'il n'existe pas
# file_name = "tweets.json"
# file_name = "vide.txt"
# if file_name not in client.list("/"):
#     with client.write("/"+file_name) as writer:
#         writer.write(b"Allez !")

print(f"listening on \"{TOPIC}\"...")
for message in consumer:
    tweet = message.value
    tweet["details"], tweet["overall_feeling"] = sentiment_scores(tweet['text'])
    print(tweet)

    # with client.write("/"+file_name , append=True) as f:
    #     f.write((json.dumps(tweet) + "\n").encode('utf-8'))