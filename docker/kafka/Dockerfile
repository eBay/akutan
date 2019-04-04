FROM ubuntu:16.04
RUN apt-get update && apt-get install -y \
    curl \
    netcat \
    openjdk-9-jdk-headless

# ZooKeeper needs /usr/lib/jvm/java-9-openjdk-amd64/conf/management/management.properties.
# Ubuntu has this in lib instead of conf, and Docker doesn't support symlinks.
RUN cp -a /usr/lib/jvm/java-9-openjdk-amd64/lib \
          /usr/lib/jvm/java-9-openjdk-amd64/conf

ENV KAFKA_VERSION 2.11-1.0.1
ENV KAFKA_DOWNLOAD_PATH 1.0.1/kafka_$KAFKA_VERSION.tgz
RUN curl -sfSLO https://archive.apache.org/dist/kafka/$KAFKA_DOWNLOAD_PATH
RUN tar -xzf kafka_$KAFKA_VERSION.tgz && mv kafka_$KAFKA_VERSION kafka

ADD docker/init.sh /kafka/init.sh
ADD docker/server.properties /kafka/config/server.properties
ADD docker/zookeeper.properties /kafka/config/zookeeper.properties
EXPOSE 2181
EXPOSE 9092
ENTRYPOINT /kafka/init.sh
