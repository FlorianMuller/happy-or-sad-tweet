import os
import time
import json
from dotenv import load_dotenv
import tweepy
from kafka import KafkaProducer

# Waiting for zookeeper and kafka to start (takes ~ 10 sec)
time.sleep(40)

# import logging
# logging.basicConfig(level=logging.DEBUG)

# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"


def define_twitter_callbacks(twitter_stream, on_tweet_fn):
    twitter_stream.on_connect = lambda: print("connected")
    twitter_stream.on_disconnect = lambda: print("disconnect")

    twitter_stream.on_tweet = on_tweet_fn
    twitter_stream.on_error = lambda error: print("error", error)


def reset_rules(twitter_stream):
    current_rules = twitter_stream.get_rules().data or []
    for rule in current_rules:
        twitter_stream.delete_rules(rule.id)


def define_twitter_rules(twitter_stream, wanted_rules):
    missing_rules = wanted_rules.copy()
    missing_rules_values = [rule.value for rule in missing_rules]

    current_rules = twitter_stream.get_rules().data or []
    for rule in current_rules:
        if rule.value not in missing_rules_values:
            # Removing unwatned rules
            twitter_stream.delete_rules(rule.id)
        else:
            # Removing good rule from missing rules
            idx = missing_rules_values.index(rule.value)
            del missing_rules_values[idx]
            del missing_rules[idx]
    
    # Adding missing rules
    for wrule in missing_rules:
        twitter_stream.add_rules(wrule)




def main():
    twitter_stream = tweepy.StreamingClient(os.environ["TWITTER_BEARER_TOKEN"])
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    
    # Callbacks
    def send_tweet(tweet):
        #print(json.dumps(tweet.data, indent=4, sort_keys=True))
        #print("~~~~~~~~~~~~~~~~")

        producer.send(TOPIC, tweet.data)

    define_twitter_callbacks(twitter_stream, send_tweet)

    # Twitter stream rules
    define_twitter_rules(twitter_stream, [
        tweepy.StreamRule(f"{os.environ['TWITTER_KEYWORD']} lang:en -is:retweet")
    ])

    print("Streaming with rules:")
    for rule in twitter_stream.get_rules().data:
        print('-', rule)

    try:
        # Start streaming tweet
        twitter_stream.filter(
            # tweet_fields=["id", "text", "created_at", "author_id", "attachments", "entities", "geo", "lang"]
            tweet_fields=["id", "text", "created_at", "author_id", "lang"]
        )
    except KeyboardInterrupt:
        producer.flush()


if __name__ == "__main__":
    main()
