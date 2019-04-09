#!/usr/bin/env bash
# this script builds the beam build docker image once the requisite packages have been
# sorted out by underlying package management
#
# it supports install of golang, protoc, and rocksdb and requires their
# versions be defined as the following in the environment:
#  GOLANGVERSION
#  PROTOCVERSION
#  ROCKSDBVERSION
#  DOCKERAPIVERSION
if [ 0 != ${EUID} ]; then
	printf "must run as root\n"
	exit 1
fi
if [ -z "${GOLANGVERSION}" ]; then
	printf "must set GOLANGVERSION\n"
	exit 1
fi
if [ -z "${PROTOCVERSION}" ]; then
	printf "must set PROTOCVERSION\n"
	exit 1
fi
if [ -z "${ROCKSDBVERSION}" ]; then
	printf "must set ROCKSDBVERSION\n"
	exit 1
fi
if [ -z "${DOCKERAPIVERSION}" ]; then
	printf "must set DOCKERAPIVERSION\n"
	exit 1
fi

# golang
curl -LO https://storage.googleapis.com/golang/go$GOLANGVERSION.linux-amd64.tar.gz
rm -rf /usr/local/go
tar -C /usr/local -xzf go$GOLANGVERSION.linux-amd64.tar.gz go
for b in /usr/local/go/bin/*; do
  ln -fs $b /usr/local/bin/
done
go version

# protoc
curl -LO https://github.com/google/protobuf/releases/download/v$PROTOCVERSION/protoc-$PROTOCVERSION-linux-x86_64.zip
DIR=protoc-$PROTOCVERSION
mkdir -p $DIR
(cd $DIR && unzip -uo ../protoc-$PROTOCVERSION-linux-x86_64.zip)
chmod -R go+rX $DIR
cp -v --preserve=mode $DIR/bin/* /usr/local/bin/
cp -rv --preserve=mode $DIR/include/* /usr/local/include/
protoc --version

./rocksdb.sh

# in order for the latest version of the docker client to interact with
# the older version of the daemon, set the version in the environment
cat >/etc/profile.d/docker.sh <<EOF
export DOCKER_API_VERSION=${DOCKERAPIVERSION}
EOF
chmod 444 /etc/profile.d/docker.sh
