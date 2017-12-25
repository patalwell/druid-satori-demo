#!/usr/bin/python

from __future__ import print_function

from getopt import getopt
import json
import kafka
import sys
import time

from satori.rtm.client import make_client, SubscriptionMode

ENDPOINT = "wss://open-data.api.satori.com"
APPKEY = "62c9e8A829Ae1Fa26cbBAd1409aC5EaA"
CHANNEL = 'cryptocurrency-market-data'

def main():
    kafka_endpoint = ['pathdp3.field.hortonworks.com:6667']
    kafka_topic = CHANNEL
    kafka_producer = kafka.KafkaProducer(bootstrap_servers=kafka_endpoint)
    kafka_consumer = kafka.KafkaConsumer(kafka_topic,
                                         bootstrap_servers=kafka_endpoint)

    with make_client(endpoint=ENDPOINT, appkey=APPKEY) as client:
        print('Connected to Satori RTM!')

        class SubscriptionObserver(object):
            def on_subscription_data(self, data):
                for message in data['messages']:
                    # print("Got message:", json.dumps(message))
                    print("Calling Kafka")
                    # produce json messages
                    future = kafka_producer.send(kafka_topic, bytes(json.dumps(
                        message)))
                    try:
                        record_metadata = future.get(timeout=10)
                    except kafka.KafkaError:
                        # Decide what to do if produce request failed...
                        log.exception("/Users/pnalwell/logs")
                        pass

                    # Successful result returns assigned partition and offset
                    print(record_metadata.topic)
                    print(record_metadata.partition)
                    print(record_metadata.offset)

                    #get messages from topic
                for message in kafka_consumer:
                    # message value and key are raw bytes -- decode if necessary!
                    # e.g., for unicode: `message.value.decode('utf-8')`
                    print("%s:%d:%d: key=%s value=%s" % (
                    message.topic, message.partition,
                    message.offset, message.key,
                    message.value))

        subscription_observer = SubscriptionObserver()
        client.subscribe(
            CHANNEL,
            SubscriptionMode.SIMPLE,
            subscription_observer)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
