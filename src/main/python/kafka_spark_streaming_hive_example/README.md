## Kafka + Spark + Hadoop + Hive
#####  In this example we will use Kafka to pull tweets from twitter and using Spark will store those tweets in Hive table

### Install Kafka
Download Kafka from official website https://kafka.apache.org/downloads and run the following steps to run zookeeper and broker and download kafka-python dependency
~~~~
1) Unzip the downloaded tgz file 
2) tar -xvf kafka_2.11-2.2.0.tgz
3) mv kafka_2.11-2.2.0/ /opt/kafka/2.2.0/
4) cd /opt/kafka/2.2.0/
5) Start zookeeper i.e. bin/zookeeper-server-start.sh config/zookeeper.properties
6) Start Kafka broker server i.e. bin/kafka-server-start.sh config/server.properties
7) Check for any topics using bin/kafka-topics.sh --list --bootstrap-server localhost:9092
8) Create new topic bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic tweets
9) Check for any topics using bin/kafka-topics.sh --list --bootstrap-server localhost:9092
10) pip3 install kakfa-python
11) pip3 install tweepy
~~~~
#### Install Hadoop
Download Hadoop from official website https://archive.apache.org/dist/hadoop/common/
~~~~
Follow steps here https://www.guru99.com/how-to-install-hadoop.html for setup
Start all the services by running sbin/start-all.sh or start-dfs.sh
~~~~
### Install Hive
Download Hive from official website https://archive.apache.org/dist/hive/ and run the following commands to setup
~~~~
1) Unzip the downloaded tgz file
2) tar -xvf apache-hive-2.3.5-bin.tar.gz
3) mv apache-hive-2.3.5-bin/* /opt/hive/2.3.5/
4) Set Hive_Home
     export HIVE_HOME=/opt/hive/2.3.5
     export PATH=$PATH:$HIVE_HOME/bin
5) Check hive --version
6) Create home directory for hive and warehouse directory
    su - hduser
    hadoop fs -mkdir /user/
    hadoop fs -mkdir /user/hive/
    hadoop fs -mkdir /user/hive/warehouse/
    hadoop fs -mkdir /tmp
    hadoop fs -chmod 777 /user/hive/warehouse/
    hadoop fs -chmod 777 /tmp
7) vi hive-env.sh
    export HADOOP_HOME=/opt/hadoop/2.8.5
    export HADOOP_HEAPSIZE=512
    export HIVE_CONF_DIR=/opt/hive/2.3.5/conf
8) vi hive-site.xml
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
        <property>
            <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/opt/hive/2.3.5/metastore_db;create=true</value>
            <description>JDBC connect string for a JDBC metastore.</description>
        </property>
        <property>
            <name>hive.metastore.warehouse.dir</name>
            <value>/user/hive/warehouse</value>
            <description>location of default database for the warehouse</description>
        </property>
        <property>
            <name>hive.metastore.uris</name>
            <value>thrift://localhost:9083</value>
            <description>Thrift URI for the remote metastore.</description>
        </property>
        <property>
            <name>javax.jdo.option.ConnectionDriverName</name>
            <value>org.apache.derby.jdbc.EmbeddedDriver</value>
            <description>Driver class name for a JDBC metastore</description>
        </property>
        <property>
            <name>javax.jdo.PersistenceManagerFactoryClass</name>
            <value>org.datanucleus.api.jdo.JDOPersistenceManagerFactory</value>
            <description>class implementing the jdo persistence</description>
        </property>
        <property>
            <name>hive.server2.enable.doAs</name>
            <value>false</value>
        </property>
    </configuration>
9) Since Hive and Kafka are running on the same system, you'll get a warning message about some SLF4J logging file. From your Hive home you can just rename the file i.e. mv lib/log4j-slf4j-impl-2.6.2.jar lib/log4j-slf4j-impl-2.6.2.jar.bak
10) hive
11) create a database schema for Hive to work with using schematool i.e. schematool -initSchema -dbType derby
12) start the Hive Metastore i.e. hive --services metastore
13) CREATE TABLE tweets (text STRING, words INT, length INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' STORED AS TEXTFILE;
14) select count(*) from tweets; -- 0 

~~~~
### Install Spark
Download Spark from official website https://archive.apache.org/dist/spark/ and run the following commands to setup
~~~~
1) unzip the downloaded tgz file i.e. tar -xvf spark-2.4.3-bin-hadoop2.7.tgz
2) mv mv spark-2.4.3-bin-hadoop2.7/* /opt/spark/2.4.3/
3) pip3 install pyspark==2.4.3
4) check version of the scala and download same verion of kafka spark streaming jar from https://search.maven.org/search?q=spark-streaming-kaf and copy the same into Spark jars folder 
5) Set Spark_Home in ~/.bashrc 
    export SPARK_HOME=/opt/spark/2.4.3
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin 
~~~~

### How to run
~~~~
1) Create twitter developer account and grab the consumer and secret keys.
2) Start zookeeper service cd /opt/kafka/2.2.0/ and then run bin/zookeeper-server-start.sh config/zookeeper.properties
3) Start Kafka broker cd /opt/kafka/2.2.0/ and then run bin/kafka-server-start.sh config/server.properties
4) Start hadoop cd /opt/hadoop/2.8.5 and then run sbin/start-all.sh
5) Start hive cd /opt/hive/2.3.5 and then schematool -initSchema -dbType derby -> hive --services metastore 
6) Place the twitter key in twitter_stream.py file and start tha(producer) i.e. python3 twitter_stream.py
7) Submit transformer.py spark application i.e. spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar transformer.py
8) select count(*) from tweets;
