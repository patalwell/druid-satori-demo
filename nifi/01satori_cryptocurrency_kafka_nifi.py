#!/usr/bin/env python

"""
Kafka Application that parses Incoming Satori dictionary ,filters for
None, and casts dataTypes from String to their respective types prior to
sending data to Kafka Broker and writing to Kafka Topic

Sample Payload:
{
"exchange":"HitBTC"
,"cryptocurrency":"OCN"
,"basecurrency":"ETH"
,"type":"market"
,"price":"0.00002300"
,"size":"null"
,"bid":"0.00002186"
,"ask":"0.00002647"
,"open":"0.00002799"
,"high":"0.00002772"
,"low":"0.00002300"
,"volume":"96000"
,"timestamp":"1522785104"
}
"""

from __future__ import print_function
from getopt import getopt
import simplejson
import kafka
import sys
import time
from decimal import Decimal as dec
from satori.rtm.client import make_client, SubscriptionMode

KAFKA_BROKER="pathdf3.field.hortonworks.com:6667"
ENDPOINT= "wss://open-data.api.satori.com"
APP_KEY= "62c9e8A829Ae1Fa26cbBAd1409aC5EaA"
CHANNEL = "cryptocurrency-market-data"
KAFKA_TOPIC= "cryptocurrency-nifi-data"


def main():
    # Parse options and setup Kafka.
    try:
        opts, args = getopt(sys.argv[1:], "k:t:")
    except Exception, e:
        assert False, "Usage: satori_cryptocurrency_kafka [-k kafka_endpoint] [-t kafka_topic]"
    kafka_endpoint = KAFKA_BROKER
    kafka_topic = KAFKA_TOPIC
    for (k,v) in opts:
        if k in ['-k']:
            kafka_endpoint = v
        elif k in ['-t']:
            kafka_topic = v
    kafka_producer = kafka.KafkaProducer(bootstrap_servers=kafka_endpoint)

    # Satori API Connection and client class object
    with make_client(endpoint=ENDPOINT, appkey=APP_KEY) as client:
        class SubscriptionObserver(object):

            def on_subscription_data(self, data):
                for message in data['messages']:
                    if message['price'] is None:
                        message['price'] = 'NaN'
                    else:
                        message['price'] = dec(message['price'])
                    if message['bid'] is None:
                        message['bid'] = 'NaN'
                    else:
                        message['bid'] = dec(message['bid'])
                    if message['ask'] is None:
                        message['ask'] = 'NaN'
                    else:
                        message['bid'] = dec(message['bid'])
                    if message['open'] is None:
                        message['open'] ='NaN'
                    else:
                        message['open'] = dec(message['open'])
                    if message['high'] is None:
                        message['high'] = "NaN"
                    else:
                        message['high'] = dec(message['high'])
                    if message['low'] is None:
                        message['low'] = 'NaN'
                    else:
                        message['low'] = dec(message['low'])
                    if message['volume'] is None:
                        message['volume'] ='NaN'
                    else:
                        message['low'] = dec(message['volume'])
                    if message['timestamp'] is None:
                        message['timestamp'] = long(time.time())
                    # print(message)

                kafka_producer.send(kafka_topic, bytes(simplejson.dumps(
                    message)))
                main.count += 1

        subscription_observer = SubscriptionObserver()
        client.subscribe(
            CHANNEL,
            SubscriptionMode.SIMPLE,
            subscription_observer)

        try:
            while True:
                time.sleep(1)
                print("Written {0} messages to Kafka".format(main.count))
        except KeyboardInterrupt:
            pass
main.count = 0

if __name__ == '__main__':
    main()
