# -*- coding: utf-8 -*-
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

#Twitter Stream as Data source
tweets = []
all_tweets = open("twitter_140.txt")  
for twts in all_tweets:  
    tweets.append(twts.replace('\n',''))
corpus = []
import re
for i in range(0,400):
    clean_text = re.sub(r'\W', ' ', str(tweets[i]))
    clean_text = re.sub(r'^br$', ' ', clean_text)
    clean_text = re.sub(r'\s+^br$\s+', ' ', clean_text)
    clean_text = re.sub(r'\s+[a-z]\s+', ' ', clean_text)
    clean_text = re.sub(r'^b\s+', ' ', clean_text)
    clean_text = re.sub(r'\s+', ' ', clean_text) 
    clean_text = clean_text.lower()    
    corpus.append(clean_text)    
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer, CountVectorizer
countvector    = CountVectorizer(stop_words= stopwords.words('english')) 
tfidftransform = TfidfTransformer()
X = countvector.fit_transform(corpus).toarray() 
X_train = tfidftransform.fit_transform(X).toarray()
train_data = []
label = [0]*200+[4]*200
for i in range(len(tweets)):
    train_data.append((list(X_train[i]),label[i]))
import pickle
import redis
import logging
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import BernoulliNB
BNB = BernoulliNB()
BNB.fit(X_train[:2],label[:2])
r = redis.StrictRedis(host='localhost', port=6379, db=0)
try:
    r.set('osamodel', pickle.dumps(BNB, protocol=pickle.HIGHEST_PROTOCOL))
    print('model initialized')
except (redis.exceptions.RedisError, TypeError, Exception):
    logging.warning('无法连接 Redis 以存储模型数据')
    print('failed')



from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink

def train_predict(X_train,label):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    called_model = pickle.loads(r.get('osamodel'))
    called_model.partial_fit([X_train],[int(label)])
    a ='model updated'
    return a

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
ds = env.from_collection(collection=train_data) #    type_info=Types.ROW([Types.PRIMITIVE_ARRAY(Types.DOUBLE()), Types.INT()])
ds.map(lambda x: train_predict(x[0],x[1]))\
  .print()
env.execute("osa_job")
