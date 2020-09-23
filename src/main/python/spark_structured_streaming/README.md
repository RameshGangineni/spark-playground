## Kafka + Spark Streaming
##### Spark Version: 2.4.3
##### Kafka Version: 2.2.0
##### Python Version: 3.7.4

~~~~
Download kafka spark sql dependency from here https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.11/2.4.3
Download kafka client from here https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.2.0/
cp spark-sql-kafka-0-10_2.11-2.4.3.jar to SPARK_HOME JARS folder
cp kafka-clients-2.2.0.jar to SPARK_HOME JARS folder

~~~~

#### How to Run

~~~~
1) Start zookeeper service cd /opt/kafka/2.2.0/ and then run bin/zookeeper-server-start.sh config/zookeeper.properties
2) Start Kafka broker cd /opt/kafka/2.2.0/ and then run bin/kafka-server-start.sh config/server.properties
3) Create Kafka testtopic and outputtopic using commands
   i)   cd /opt/kafka/2.2.0  
   ii)  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testtopic
   iii) bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic outputtopic
   iv)  Check for topics using bin/kafka-topics.sh --list --bootstrap-server localhost:9092
4) Start kafka consumer i.e. cd /opt/kafka/2.2.0 and then bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outputtopic 
5) Submit Spark application i.e. run spark_structured_streaming_kafka.py in pycharm itself
6) Start Kafka producer i.e. kafka_producer.py in pycharm itself, it will send 5 messages and we can validate the same in kafka consumer and spark streaming console

~~~~