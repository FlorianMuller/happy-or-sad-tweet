import os
import json
from dotenv import load_dotenv
import tweepy
from kafka import KafkaProducer

# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"


def define_twitter_callbacks(twitter_stream, on_tweet_fn):
    twitter_stream.on_connect = lambda: print("connected")
    twitter_stream.on_disconnect = lambda: print("disconnect")

    twitter_stream.on_tweet = on_tweet_fn
    twitter_stream.on_error = lambda error: print("error", error)


def define_twitter_rules(twitter_stream):
    twitter_stream.add_rules(tweepy.StreamRule(os.environ["TWITTER_KEYWORD"]))
    twitter_stream.add_rules(tweepy.StreamRule("lang:en"))
    twitter_stream.add_rules(tweepy.StreamRule("-is:retweet"))


def main():
    twitter_stream = tweepy.StreamingClient(os.environ["TWITTER_BEARER_TOKEN"])
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    
    # Callbacks
    def send_tweet(tweet):
        print(json.dumps(tweet.data, indent=4, sort_keys=True))
        print("~~~~~~~~~~~~~~~~")

        producer.send(TOPIC, tweet.data)

    define_twitter_callbacks(twitter_stream, send_tweet)

    # Twitter stream rules
    define_twitter_rules(twitter_stream)
    
    try:
        # Start streaming tweet
        twitter_stream.filter()
    except KeyboardInterrupt:
        producer.flush(
            # tweet_fields=["id", "text", "created_at", "author_id", "attachments", "entities", "geo", "lang"]
        )


if __name__ == "__main__":
    main()
