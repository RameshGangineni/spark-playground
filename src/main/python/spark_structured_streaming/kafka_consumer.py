from common.pretty_print import *

from kafka import KafkaConsumer
from json import loads
import time
import pandas as pd

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = '127.0.0.1:9092'

if __name__ == "__main__":
    print("Kafka Consumer Application Started ... ")
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME_CONS,
                             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             auto_offset_reset='latest',
                             enable_auto_commit=True,
                             group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
                             value_deserializer=lambda x: loads(x.decode('utf-8')))

    def get_message_df():
        print_info("Reading Messages from Kafka Topic about to Start ... ")
        counter = 0
        df = pd.DataFrame()
        for message in consumer:
            print_warn("Key: ", message.key)
            output_message = message.value
            df.append(output_message, [counter])
            counter += 1
            print_info("Counter in for loop: ", counter)
            if counter == 10:
                print_info("Counter in if loop: ", counter)
                yield df
                counter = 0
                time.sleep(5)

    for df in get_message_df():
        print_info("Before DataFrame Head ...")
        df.head()
        print_info("After DataFrame Head.")
