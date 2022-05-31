import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'])

tweets_fait_maing = ['I feel bad for Felix','i hate flowers',
                     'hes kind and smart','we are kind to good people',
                     'He is a genius', 'I am not happy with you.',
                     "Yesterday i was very happy. But I am very sad today."]

for data in tweets_fait_maing:
    producer.send('data', str(data).encode('utf-8'))
    print(data)
    time.sleep(1)