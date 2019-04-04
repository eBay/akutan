#!/bin/bash
set -e
cd /kafka
echo "Starting ZooKeeper"
(
    ./bin/zookeeper-server-start.sh config/zookeeper.properties 2>&1 |
    while read -r line; do
        echo "[ZooKeeper] $line" >&2
    done
) &
echo "Starting Kafka"
(
    while ! nc -z localhost 2181; do
        echo "[Zookeeper] Starting..."
        sleep 0.1 # wait for 1/10 of the second before check again
    done

    # By default, Kafka will advertise $(hostname):9092 through ZooKeeper to
    # clients. However, this isn't routable in Kubernetes. We instead listen on
    # its IP, which is routable within the Kubernetes cluster.
    echo "listeners=PLAINTEXT://$(hostname --ip-address):9092" >> config/server.properties

    ./bin/kafka-server-start.sh config/server.properties 2>&1 |
    while read -r line; do
        echo "[Kafka] $line" >&2
    done
) &
wait
