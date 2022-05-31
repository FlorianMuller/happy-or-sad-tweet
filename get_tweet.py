import os
import json
from dotenv import load_dotenv
import tweepy
from kafka import KafkaProducer

# take environment variables from .env.
load_dotenv()

KEYWORD="ukraine"

KAFKA_URL = "localhost"
KAFKA_BROKERS = [f"{KAFKA_URL}:9092", f"{KAFKA_URL}:9093", f"{KAFKA_URL}:9094"]
TOPIC = f"{KEYWORD}_tweet"


def define_twitter_callbacks(twitter_stream, on_tweet_fn):
    twitter_stream.on_connect = lambda: print("connected")
    twitter_stream.on_disconnect = lambda: print("disconnect")

    twitter_stream.on_tweet = on_tweet_fn
    twitter_stream.on_error = lambda error: print("error", error)


def define_twitter_rules(twitter_stream):
    twitter_stream.add_rules(tweepy.StreamRule(KEYWORD))
    twitter_stream.add_rules(tweepy.StreamRule("lang:en"))
    twitter_stream.add_rules(tweepy.StreamRule("-is:retweet"))


def main():
    twitter_stream = tweepy.StreamingClient(os.environ["TWITTER_BEARER_TOKEN"])
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )
    
    # Callbacks
    def send_tweet(tweet):
        print(tweet.data)
        print("~~~~~~~~~~~~~~~~")

        producer.send(TOPIC, tweet.data)

    define_twitter_callbacks(twitter_stream, send_tweet)

    # Twitter stream rules
    define_twitter_rules(twitter_stream)
    
    try:
        # Start streaming tweet
        twitter_stream.filter()
    except KeyboardInterrupt:
        producer.flush()


if __name__ == "__main__":
    main()
