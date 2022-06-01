from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka import KafkaConsumer
import hdfs

client = hdfs.InsecureClient("http://192.168.49.1:50070")

consumer = KafkaConsumer(
    'data',
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    group_id='users-group'
)

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

for message in consumer:
    with client.write("/tweets.csv", append=True) as f:
        reponse = message.value.decode()
        details, overall_feeling = sentiment_scores(reponse)
        data = reponse + ',' + str(overall_feeling) + ',' + str(details) + '\n'
        print(data)
        f.write(data.encode('utf-8'))