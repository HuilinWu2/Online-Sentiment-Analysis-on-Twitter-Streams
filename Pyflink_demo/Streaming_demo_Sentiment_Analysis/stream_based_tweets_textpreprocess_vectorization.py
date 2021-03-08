# -*- coding: utf-8 -*-

#Twitter Stream as Data source
twitter_stream= []
tweets = []
label = []
all_tweets = open("twitter_140.txt")  
for twts in all_tweets:  
    tweets.append(twts.replace('\n',''))
label = [0]*200+[4]*200
for i in range(len(tweets)):
    twitter_stream.append((tweets[i],label[i]))

# def load_model():
#     import redis
#     import pickle
#     import logging
#     from sklearn.naive_bayes import BernoulliNB

#     r = redis.StrictRedis(host='localhost', port=6379, db=0)
#     try:
#         called_model = pickle.loads(r.get('osamodel'))
#     except TypeError:
#         logging.info('The model name you entered cannot be found in redis, now we initialize a model')
#     except (redis.exceptions.RedisError, TypeError, Exception):
#         logging.warning('There is a problem with Redis, now we initialize a model')
#     finally:
#         called_model = called_model or BernoulliNB

#     return called_model


def vectorization(text,label):
    from nltk.corpus import stopwords
    from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer, CountVectorizer
    countvector    = CountVectorizer(stop_words= stopwords.words('english')) 
    tfidftransform = TfidfTransformer()
    X = countvector.fit_transform([text]).toarray() 
    X_train = tfidftransform.fit_transform(X).toarray()
    return (X_train,label)

from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink

def textprocess(text,label):
    import re
    text = re.sub(r'\W', ' ', text)
    text = re.sub(r'^br$', ' ', text)
    text = re.sub(r'\s+^br$\s+', ' ', text)
    text = re.sub(r'\s+[a-z]\s+', ' ', text)
    text = re.sub(r'^b\s+', ' ', text)
    text = re.sub(r'\s+', ' ', text) 
    text = text.lower().strip()
    vectorization(text,label)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
ds = env.from_collection(
    collection=twitter_stream,
    type_info=Types.ROW([Types.STRING(), Types.INT()]))
ds.map(lambda x: textprocess(x[0],x[1]))\
  .print()
env.execute("tutorial_job")
