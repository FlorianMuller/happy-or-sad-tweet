import os
import json
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import eng_spacysentiment
from afinn import Afinn
from kafka import KafkaConsumer
import hdfs

# Initialize Afinn Engine
afinn = Afinn()

# Load Spacy NLP english engine
spacy_nlp = eng_spacysentiment.load()

# Create a SentimentIntensityAnalyzer object.
sid_obj = SentimentIntensityAnalyzer()
 

# take environment variables from .env
load_dotenv()
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
#TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"
TOPIC = "twitter-topic"

# Compute Afinn sentiment version
def afinn_sentiment(afinn,text):
  score = afinn.score(text)
  if abs(score) < 0.5:
    # Neutral (=> between -0.5 and 0.5, through abs function)
    feeling = "Neutral"
  elif score > 0.5:
    # Positive
    feeling = "Positive"
  else:
    # Negative
    feeling = "Negative"

  return score, feeling

# Compute spacy sentiment version
def spacy_sentiment(nlp, text):
  doc = nlp(text)
  if doc.cats["positive"] > doc.cats["negative"]:
    feeling = "Positive"
  elif doc.cats["negative"] > doc.cats["positive"]:
    feeling = "Negative"
  else:
    feeling = "Neutral"

  return doc.cats, feeling

# Compute vader sentiment version
def vader_sentiment(sid_obj, sentence):
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

#Creation du fichier s'il n'existe pas
file_name = "tweets.json"
if file_name not in client.list("/data"):
    with client.write("/data/"+file_name):
        pass

print(f"listening on \"{TOPIC}\"...")
for message in consumer:
    tweet = message.value
    print(tweet)
    tweet["details"], tweet["overall_feeling"] = vader_sentiment(sid_obj, tweet['text'])
    tweet["details_spacy"], tweet["spacy_feeling"] = spacy_sentiment(spacy_nlp, tweet['text'])
    tweet["details_afinn"], tweet["afinn_feeling"] = afinn_sentiment(afinn, tweet['text'])


    with client.write("/data/"+file_name , append=True) as f:
        f.write((json.dumps(tweet) + "\n").encode('utf-8'))
