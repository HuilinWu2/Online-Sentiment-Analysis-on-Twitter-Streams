# Online-Sentiment-Analysis-on-Twitter-Streams
Master Thesis of ,[DIMA](https://www.dima.tu-berlin.de/menue/database_systems_and_information_management_group/?no_cache=1), TU Berlin->An Empirical Study of  Online Sentiment Analysis on Twitter Streams 
## Motivation
* Most existing studies regarding Sentiment Analysis are based on offline batch-based learning mechanisms. Meanwhile, many stream processing systems have been proposed, but they are not specifically designed for online learning tasks, such as online Sentiment Analysis. As a result, it still remains an open and challenging question of how to efficiently perform Sentiment Analysis for real-time streaming data, e.g., ongoing Twitter Streams.
* The goal of this thesis is to empirically evaluate various online algorithms for Sentiment Analysis on Twitter Streams by implementing them on DSPS (Data Stream Processing System) for practical application.

## Environment Requirement
1. Flink v1.11
2. Scala v2.11
3. Python 3.7
4. Java 8
5. Kafka
6. Redis server

## DataSource
1.6 million labeled Tweets:
[Sentiment140](http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip)

## Folder Guide
1. Pyflink_demo
* Tweets_clean_demo: Demo for batch-based Tweets preprocessing for Sentiment Analysis on FLink
* Streaming_demo_Sentiment_Analysis: Demo for stream-based Tweets preprocessing & online Sentiment Analysis model on Flink
2. OSA_algorithms
* Algorithms of incremental Sentiment Analysis
3. Python_small_job
* Python file of various developing note
