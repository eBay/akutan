#!/usr/bin/env bash
# This script is used to fetch / build RocksDB.
# $ROCKSDBVERSION must be set.
# This script is used by both the Docker build image and the travis configuration

[ -d rocksdb/.git ] && (cd rocksdb && git fetch origin) || git clone https://github.com/facebook/rocksdb.git
(cd rocksdb && git reset --hard $ROCKSDBVERSION)

# this dance is due to git trampling the timestamps, which otherwise would cause it to rebuild everything every time.
if [ ! -f rocksdb/built_version.txt ] || [[ $(< rocksdb/built_version.txt) != $ROCKSDBVERSION ]]; then
    echo "Building RocksDB $ROCKSDBVERSION"
    (cd rocksdb && EXTRA_CFLAGS='-fPIC' EXTRA_CXXFLAGS='-fPIC' make -j 2 install-static)
    echo $ROCKSDBVERSION > rocksdb/built_version.txt
else
    echo "RocksDB $ROCKSDBVERSION already built"
    (cd rocksdb && NO_UPDATE_BUILD_VERSION=1 make install-headers)
    (cd rocksdb && install -C -m 755 librocksdb.a /usr/local/lib)
fi
