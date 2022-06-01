import os
import json
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
import hdfs

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



client = hdfs.InsecureClient(os.environ["HDFS_URL"])

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    group_id='users-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    with client.write("/tweets.csv", append=True) as f:
        tweet = message.value
        tweet_text = tweet.text

        tweet["details"], tweet["overall_feeling"] = sentiment_scores(tweet_text)

        f.write((json.dumps(tweet) + "\n").encode('utf-8'))
