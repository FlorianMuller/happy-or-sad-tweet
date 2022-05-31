from dotenv import load_dotenv
import tweepy
import os

# take environment variables from .env.
load_dotenv()

KEYWORD="ukraine"

def send_tweet(tweet):
    print(tweet.data)
    print("~~~~~~~~~~~~~~~~")


def main():
    streaming_client = tweepy.StreamingClient(os.environ["TWITTER_BEARER_TOKEN"])
    # Callbacks
    streaming_client.on_connect = lambda: print("connected")
    streaming_client.on_disconnect = lambda: print("disconnect")

    streaming_client.on_tweet = send_tweet
    streaming_client.on_error = lambda error: print("error", error)

    # Stream rules
    streaming_client.add_rules(tweepy.StreamRule(KEYWORD))
    streaming_client.add_rules(tweepy.StreamRule("lang:en"))
    streaming_client.add_rules(tweepy.StreamRule("-is:retweet"))
    
    # Start streaming
    streaming_client.filter()


if __name__ == "__main__":
    main()
