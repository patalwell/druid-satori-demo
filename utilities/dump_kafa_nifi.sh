#!/bin/sh

CHANNEL=cryptocurrency-nifi-data
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper pathdp1.field.hortonworks.com:2181 --topic $CHANNEL --from-beginning
