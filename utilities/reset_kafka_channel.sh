#!/bin/sh

CHANNEL=cryptocurrency-market-data
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --zookeeper pathdp1.field.hortonworks.com:2181 --topic $CHANNEL
