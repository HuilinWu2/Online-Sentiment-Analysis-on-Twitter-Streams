# Online-Sentiment-Analysis-on-Twitter-Streams
Master Thesis of ,DIMA, TU Berlin->An Empirical Study of  Online Sentiment Analysis on Twitter Streams 
## Motivation
* Most existing studies regarding Sentiment Analysis are based on offline batch-based learning mechanisms. Meanwhile, many stream processing systems have been proposed, but they are not specifically designed for online learning tasks, such as online Sentiment Analysis. As a result, it still remains an open and challenging question of how to efficiently perform Sentiment Analysis for real-time streaming data, e.g., ongoing Twitter Streams.
* The goal of this thesis is to empirically evaluate various online algorithms for Sentiment Analysis on Twitter Streams by implementing them on DSPS (Data Stream Processing System) for practical application.

## Environment Requirement
1. Flink v1.11
2. Python 3.7
3. Java 8
4. Kafka
5. Redis server

## DataSource
1.6 million labeled Tweets:
[Sentiment140](http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip)

## Folder Guide
1. Pyflink_demo/Tweets_clean_demo -> Demo for batch-based Tweets preprocessing for Sentiment Analysis on FLink
2. xxxxxx
3. xxxxxx
