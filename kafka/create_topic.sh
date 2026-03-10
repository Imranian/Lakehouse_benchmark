#!/bin/bash

$KAFKA_HOME/bin/kafka-topics.sh \
--create \
--topic sensor_topic \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1