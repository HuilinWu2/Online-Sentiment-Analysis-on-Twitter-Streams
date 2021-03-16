import numpy as np
from json import dumps
from time import sleep
from sklearn import datasets
from datetime import datetime
from kafka import KafkaProducer

#=====================================================
msg_per_second = 10  
topic = "stream_tweets"  # kafka topic
bootstrap_servers = ['localhost:9092']

def write_data():
    tweets = []
    label = []
    all_tweets = open("twitter_140.txt")  
    for twts in all_tweets:  
        tweets.append(twts.replace('\n',''))
    label = [0]*200+[4]*200

    all_x = tweets
    all_y = label

    # producer initialized
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    while True:
        for x, y in zip(all_x, all_y):
            # produce data and send to kafka
            cur_data = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "x": x,
                "actual_y": int(y)
            }
            producer.send(topic, value=cur_data)
            sleep(1 / msg_per_second)

if __name__ == '__main__':
    write_data()
