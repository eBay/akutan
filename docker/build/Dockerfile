# Ubuntu 16.04.3 LTS (Xenial Xerus)
FROM ubuntu:16.04 as beam-builder-apt
USER root
# https://packages.ubuntu.com/xenial/allpackages?format=txt.gz
RUN apt-get update && apt-get -y install apt-transport-https \
	build-essential \
	clang-3.9 \
	cmake \
	libbz2-dev \
	libclang-3.9-dev \
	liblz4-dev \
	liblz4-tool \
	libsnappy-dev \
	libssl-dev \
	libzstd-dev \
	llvm-3.9-dev \
	pkg-config \
	r-base-core \
	software-properties-common \
	unzip \
	vim \
	zlib1g-dev

# https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
RUN apt-get update && apt-get -y install docker-ce

FROM beam-builder-apt as beam-builder

LABEL description="This is a builder image to build beam."
ENV GOLANGVERSION 1.11.5
ENV PROTOCVERSION 3.5.1
ENV ROCKSDBVERSION v5.10.2
ENV DOCKERAPIVERSION 1.21
COPY ./docker-beam-builder.sh .
COPY ./rocksdb.sh .
RUN ./docker-beam-builder.sh
